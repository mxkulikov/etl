# ETL-сервис — заметки по реализации

## Стек и почему

| Компонент | Выбор | Почему |
|---|---|---|
| Конфиг | `yaml.v3` + env-override | комментарии в YAML, секреты только из env (`yaml:"-"` в коде специально) |
| PostgreSQL | `pgx/v5` + `pgxpool` | стандарт индустрии, нативный драйвер, пул соединений, лучше `database/sql` по производительности |
| gRPC | `google.golang.org/grpc` + `protoc` | стандарт, кодогенерация из `api/proto/records.proto` |
| S3 | `minio-go/v7` | S3-совместимый, работает с MinIO и AWS S3 |
| Kafka | `segmentio/kafka-go` | стандарт индустрии |
| Логи | `log/slog` (stdlib) | структурированные JSON-логи без зависимостей (вообще хотел, но не хватило времени реализовать zap logger from context с разными цветными уровнями логрования) |

## Структура

- `models/` — типы (`RawRecord`, `Result`, `ResultMessage`) и порты (`Extractor`, `Transformer`, `Loader`, `Publisher`).
- `adapters/` — реализации портов: `extractor/` (file, pg, grpc), `loader/` (s3), `publisher/` (kafka).
- `transformer/`, `pipeline/` — ядро, без внешних зависимостей.
- `api/proto/` — gRPC-контракт (публичный), сгенерённый код рядом с `.proto`.
- `cmd/etl/` — сборка всего в `main`.

Направление зависимостей: `adapters` -> `models` <- `pipeline`/`transformer`. Ядро не знает про адаптеры.

## Допущения (не указаны в ТЗ)

1. **Источники совместимы по схеме**: file/pg/grpc отдают `id`, `name`, `amount`. Трансформер общий.
2. **gRPC-контракт придуман**: `RecordService.GetRecords` минимальный, достаточный для ETL. При наличии реального `.proto` замена.
3. **TLS выключен** для простоты локального запуска (PG `sslmode=disable`, gRPC `insecure`, MinIO `UseSSL=false`). В проде включить.
4. **Kafka `RequireAll`**: для single-node локально может зависнуть -> допустимо `RequireOne`.
5. **Best-effort на битых записях**: пустой `id`/`name`/нечисловой `amount` -> skip + лог. Политика `fail_fast` — TODO (конфигурируемо).
6. **`schema_version`** в `input.json` только логируется; миграция при `> 1` — TODO.
7. **Пустой батч не пропускается**: S3-объект пишется, Kafka-сообщение с `count: 0` — downstream знает о прогоне.
8. **`processed_at` == `ProducedAt`**: момент обработки батча; если нужно различать — разделим.
9. **Источники обрабатываются последовательно** (fail fast). Параллельно через `errgroup` — TODO.
10. **`scanRecord`/`scanFileRecord`/`scanGRPCRecord`** вынесены для тестируемости (маппинг без БД/сети).

## Graceful shutdown

- Первый `SIGINT`/`SIGTERM` -> отмена контекста, штатное завершение.
- Второй -> `os.Exit(130)` (POSIX: 128 + SIGINT), аналог `docker stop`/`docker kill`.
- `os.Interrupt == syscall.SIGINT`, поэтому перечислил только уникальные сигналы.
- Buffer 2 избыточно (знаю, что стандарт 1).

## Тесты

- Покрыто: `config`, `file`/`repo`/`grpc` (маппинг), `transformer`, `pipeline`, `s3` (ключ), `kafka` (формат).
- **Не покрыто** (TODO): интеграционные тесты с реальными PG/MinIO/Kafka/gRPC, так как требуют Docker/testcontainers, что по условию не требовалось.

## Что бы улучшил при большем времени

1. Интеграционные тесты (`testcontainers-go`).
2. Параллельная обработка источников (`errgroup`).
3. Retry + DLQ для Kafka.
4. Метрики (Prometheus): длительность этапов, кол-во записей, ошибки.
5. Конфигурируемая политика `fail_fast` vs `best_effort`.
6. HTTP-сервер с `/healthz`/`/readyz` (если сервис долгоживущий, а не батчевый).
7. TLS для PG/gRPC/MinIO/Kafka.
8. Логгирование кастомным логгером с обвесами с использованием ctx.WithLogger(...) & logger.FromContext(...)

## Запуск

```bash
go build -o bin/etl.exe ./cmd/etl
./bin/etl.exe
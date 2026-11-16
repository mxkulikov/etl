package extractor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"okb/test-task/internal/models"
)

const postgresSource = "postgres"

// PostgresExtractor читает записи из таблицы source_records.
// Схема: id TEXT PK, name TEXT, amount NUMERIC(12,2), updated_at TIMESTAMPTZ.
type PostgresExtractor struct {
	pool    *pgxpool.Pool
	table   string
	timeout time.Duration
	log     *slog.Logger
}

type PGConfig struct {
	Host         string
	Port         int
	Database     string
	User         string
	Password     string
	MaxConns     int
	QueryTimeout time.Duration
	Table        string
}

func NewPostgresExtractor(cfg PGConfig, log *slog.Logger) (*PostgresExtractor, error) {
	if log == nil {
		log = slog.Default()
	}
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database,
	)
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres extractor: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = int32(cfg.MaxConns)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres extractor: create pool: %w", err)
	}
	return &PostgresExtractor{
		pool:    pool,
		table:   cfg.Table,
		timeout: cfg.QueryTimeout,
		log:     log,
	}, nil
}

func (e *PostgresExtractor) Name() string { return postgresSource }

// Закрывает пул. Вызываю из main после Run.
func (e *PostgresExtractor) Close() { e.pool.Close() }

func (e *PostgresExtractor) Extract(ctx context.Context) ([]models.RawRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	query := fmt.Sprintf(`SELECT id, name, amount FROM %s ORDER BY id`, e.table)

	rows, err := e.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("postgres extractor: query %q: %w", e.table, err)
	}
	defer rows.Close()

	now := time.Now().UTC()
	records := make([]models.RawRecord, 0)
	for rows.Next() {
		var (
			id     string
			name   string
			amount float64
		)
		if err := rows.Scan(&id, &name, &amount); err != nil {
			return nil, fmt.Errorf("postgres extractor: scan: %w", err)
		}
		if id == "" {
			e.log.Warn("skip postgres record without id", "source", postgresSource)
			continue
		}
		records = append(records, models.RawRecord{
			ID:     id,
			Source: postgresSource,
			Payload: map[string]any{
				"name":   name,
				"amount": amount,
			},
			FetchedAt: now,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres extractor: rows: %w", err)
	}

	e.log.Info("postgres extractor done",
		"source", postgresSource,
		"table", e.table,
		"accepted", len(records),
	)
	return records, nil
}

// scanRecord маппит одну строку из БД в RawRecord.
// также как и grpc.go вынес для тестируемости.
func scanRecord(id, name string, amount float64, now time.Time) (models.RawRecord, bool) {
	if id == "" {
		return models.RawRecord{}, false
	}
	return models.RawRecord{
		ID:     id,
		Source: postgresSource,
		Payload: map[string]any{
			"name":   name,
			"amount": amount,
		},
		FetchedAt: now,
	}, true
}

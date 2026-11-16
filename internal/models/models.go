package models

import "time"

// RawRecord универсальная запись, которую отдаёт любой экстрактор.
// Предполагаем, что источники совместимы по схеме.
type RawRecord struct {
	ID        string         `json:"id"`
	Source    string         `json:"source"` // "file" | "postgres" | "grpc"
	Payload   map[string]any `json:"payload"`
	FetchedAt time.Time      `json:"fetched_at"`
}

// Result то, что уходит в S3 и упоминается в Kafka-сообщении.
type Result struct {
	ID         string         `json:"id"`
	Source     string         `json:"source"`
	Data       map[string]any `json:"data"`
	ProducedAt time.Time      `json:"produced_at"`
}

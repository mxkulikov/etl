package models

import (
	"context"
	"time"
)

type Extractor interface {
	Name() string
	Extract(ctx context.Context) ([]RawRecord, error)
}

type Transformer interface {
	Transform(ctx context.Context, in []RawRecord) ([]Result, error)
}

// Loader пишет результат в S3 и возвращает ключ объекта.
// Ключ нужен, чтобы опубликовать его в Kafka только после успешной записи.
type Loader interface {
	Load(ctx context.Context, results []Result) (s3Key string, err error)
}

// Publisher публикует сигнал о новом результате.
type Publisher interface {
	Publish(ctx context.Context, msg ResultMessage) error
}

type ResultMessage struct {
	S3Key      string    `json:"s3_key"`
	Count      int       `json:"count"`
	ProducedAt time.Time `json:"produced_at"`
}

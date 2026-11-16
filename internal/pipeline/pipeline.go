package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"okb/test-task/internal/models"
)

// Pipeline оркестрирует ETL: extract -> transform -> load (S3) -> publish (Kafka).
// Зависит только от интерфейсов models, а не от конкретных адаптеров.
type Pipeline struct {
	extractor   models.Extractor
	transformer models.Transformer
	loader      models.Loader
	publisher   models.Publisher
	log         *slog.Logger
}

func New(
	extractor models.Extractor,
	transformer models.Transformer,
	loader models.Loader,
	publisher models.Publisher,
	log *slog.Logger,
) *Pipeline {
	if log == nil {
		log = slog.Default()
	}
	return &Pipeline{
		extractor:   extractor,
		transformer: transformer,
		loader:      loader,
		publisher:   publisher,
		log:         log,
	}
}

// Run выполняет один прогон пайплайна. Порядок: extract -> transform -> load -> publish.
// Публикация в Kafka выполняется только после успешной записи в S3.
// loader возвращает ключ, который передаётся в сообщение.
func (p *Pipeline) Run(ctx context.Context) error {
	started := time.Now()

	records, err := p.extractor.Extract(ctx)
	if err != nil {
		return fmt.Errorf("pipeline: extract from %s: %w", p.extractor.Name(), err)
	}
	p.log.Info("extract done",
		"source", p.extractor.Name(),
		"records", len(records),
	)

	results, err := p.transformer.Transform(ctx, records)
	if err != nil {
		return fmt.Errorf("pipeline: transform: %w", err)
	}
	p.log.Info("transform done", "results", len(results))

	s3Key, err := p.loader.Load(ctx, results)
	if err != nil {
		return fmt.Errorf("pipeline: load to s3: %w", err)
	}
	p.log.Info("load done", "s3_key", s3Key, "count", len(results))

	msg := models.ResultMessage{
		S3Key:      s3Key,
		Count:      len(results),
		ProducedAt: time.Now().UTC(),
	}
	if err := p.publisher.Publish(ctx, msg); err != nil {
		return fmt.Errorf("pipeline: publish to kafka: %w", err)
	}
	p.log.Info("publish done", "s3_key", s3Key)

	p.log.Info("pipeline run complete",
		"duration", time.Since(started).String(),
	)
	return nil
}

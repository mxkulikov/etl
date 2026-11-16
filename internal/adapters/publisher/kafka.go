package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"okb/test-task/internal/models"
)

// KafkaPublisher публикует сигнал о новом результате в Kafka.
type KafkaPublisher struct {
	writer *kafka.Writer
	log    *slog.Logger
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
}

func NewKafkaPublisher(cfg KafkaConfig, log *slog.Logger) *KafkaPublisher {
	if log == nil {
		log = slog.Default()
	}
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll, // ждём подтверждения всех ISR-реплик
			Async:        false,            // синхронно. ошибка сразу видна
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 10 * time.Second,
			ReadTimeout:  10 * time.Second,
		},
		log: log,
	}
}

// Закрывает writer. Вызываю из main после Run.
func (p *KafkaPublisher) Close() error { return p.writer.Close() }

func (p *KafkaPublisher) Publish(ctx context.Context, msg models.ResultMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("kafka publisher: marshal message: %w", err)
	}

	// Ключ = S3Key гарантирует, что сообщения об одном и том же объекте попадут в одну партицию.
	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.S3Key),
		Value: body,
	})
	if err != nil {
		return fmt.Errorf("kafka publisher: write message: %w", err)
	}

	p.log.Info("kafka publish done",
		"topic", p.writer.Topic,
		"s3_key", msg.S3Key,
		"count", msg.Count,
	)
	return nil
}

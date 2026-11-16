package publisher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"okb/test-task/internal/models"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestKafkaPublisher_MarshalMessage(t *testing.T) {
	msg := models.ResultMessage{
		S3Key:      "results/2026/11/08/abc123.json",
		Count:      5,
		ProducedAt: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
	}
	body, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decoded["s3_key"] != msg.S3Key {
		t.Fatalf("s3_key mismatch: %v", decoded["s3_key"])
	}
	if decoded["count"] != float64(msg.Count) {
		t.Fatalf("count mismatch: %v", decoded["count"])
	}
	if _, ok := decoded["produced_at"]; !ok {
		t.Fatal("produced_at missing")
	}
}

func TestKafkaPublisher_Publish_ContextCancelled(t *testing.T) {
	p := NewKafkaPublisher(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
	}, discardLogger())
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.Publish(ctx, models.ResultMessage{S3Key: "k"})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

package loader

import (
	"context"
	"testing"
	"time"

	"okb/test-task/internal/models"
)

func TestBuildObjectKey(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	key, err := buildObjectKey(now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Формат: results/2026/11/08/<16 hex>.json
	if len(key) != len("results/2026/11/08/")+16+len(".json") {
		t.Fatalf("unexpected key length: %q", key)
	}
	if key[:len("results/2026/11/08/")] != "results/2026/11/08/" {
		t.Fatalf("unexpected prefix: %q", key)
	}
	if key[len(key)-5:] != ".json" {
		t.Fatalf("unexpected suffix: %q", key)
	}
}

func TestBuildObjectKey_Uniqueness(t *testing.T) {
	now := time.Now().UTC()
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		key, err := buildObjectKey(now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, exists := seen[key]; exists {
			t.Fatalf("duplicate key: %q", key)
		}
		seen[key] = struct{}{}
	}
}

func TestS3Loader_Load_ContextCancelled(t *testing.T) {
	l, err := NewS3Loader(S3Config{
		Endpoint:  "localhost:9000",
		Bucket:    "test",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = l.Load(ctx, []models.Result{{ID: "1"}})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

package transformer

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"okb/test-task/internal/models"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestTransform_HappyPath(t *testing.T) {
	in := []models.RawRecord{
		{
			ID: "rec-1", Source: "file",
			Payload: map[string]any{"name": "  Alice  ", "amount": 100.0},
		},
	}
	out, err := New(discardLogger()).Transform(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 result, got %d", len(out))
	}
	if out[0].ID != "rec-1" || out[0].Source != "file" {
		t.Fatalf("unexpected result: %+v", out[0])
	}
	if out[0].Data["name"] != "Alice" {
		t.Fatalf("name not trimmed: %v", out[0].Data["name"])
	}
	if out[0].Data["amount"] != 100.0 {
		t.Fatalf("amount lost: %v", out[0].Data["amount"])
	}
	if out[0].Data["source"] != "file" {
		t.Fatalf("source not enriched: %v", out[0].Data["source"])
	}
	if _, ok := out[0].Data["processed_at"].(time.Time); !ok {
		t.Fatal("processed_at not set")
	}
}

func TestTransform_SkipsInvalid(t *testing.T) {
	in := []models.RawRecord{
		{ID: "ok", Source: "file", Payload: map[string]any{"name": "Alice", "amount": 10.0}},
		{ID: "empty-name", Source: "file", Payload: map[string]any{"name": "   ", "amount": 10.0}},
		{ID: "no-name", Source: "file", Payload: map[string]any{"amount": 10.0}},
		{ID: "neg-amount", Source: "file", Payload: map[string]any{"name": "Bob", "amount": -5.0}},
		{ID: "bad-amount", Source: "file", Payload: map[string]any{"name": "Bob", "amount": "not a number"}},
	}
	out, err := New(discardLogger()).Transform(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 accepted, got %d", len(out))
	}
	if out[0].ID != "ok" {
		t.Fatalf("unexpected accepted id: %s", out[0].ID)
	}
}

func TestTransform_DoesNotMutateInput(t *testing.T) {
	payload := map[string]any{"name": "Alice", "amount": 100.0}
	in := []models.RawRecord{
		{ID: "rec-1", Source: "file", Payload: payload},
	}
	if _, err := New(discardLogger()).Transform(context.Background(), in); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, exists := payload["processed_at"]; exists {
		t.Fatal("input payload was mutated")
	}
}

func TestTransform_EmptyInput(t *testing.T) {
	out, err := New(discardLogger()).Transform(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil slice, got nil")
	}
	if len(out) != 0 {
		t.Fatalf("expected 0 results, got %d", len(out))
	}
}

func TestTransform_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := New(discardLogger()).Transform(ctx, nil); err == nil {
		t.Fatal("expected context error")
	}
}

func TestToFloat64(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want float64
		ok   bool
	}{
		{"float64", 1.5, 1.5, true},
		{"int", 2, 2.0, true},
		{"int64", int64(3), 3.0, true},
		{"string", "4", 0, false},
		{"nil", nil, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := toFloat64(c.in)
			if ok != c.ok || got != c.want {
				t.Fatalf("toFloat64(%v) = (%v, %v), want (%v, %v)", c.in, got, ok, c.want, c.ok)
			}
		})
	}
}

package extractor

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestFileExtractor_HappyPath(t *testing.T) {
	e := NewFileExtractor("testdata/input.json", discardLogger())

	records, err := e.Extract(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 accepted records, got %d", len(records))
	}
	if records[0].ID != "rec-1" || records[0].Source != fileSource {
		t.Fatalf("unexpected first record: %+v", records[0])
	}
	if records[0].Payload["name"] != "Alice" {
		t.Fatalf("payload lost: %+v", records[0].Payload)
	}
	if records[0].FetchedAt.IsZero() {
		t.Fatal("FetchedAt not set")
	}
	if !records[0].FetchedAt.Equal(records[1].FetchedAt) {
		t.Fatal("FetchedAt must be equal for the whole batch")
	}
}

func TestFileExtractor_MissingFile(t *testing.T) {
	e := NewFileExtractor("testdata/does-not-exist.json", discardLogger())

	_, err := e.Extract(context.Background())
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestFileExtractor_EmptyRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"records":[]}`), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	e := NewFileExtractor(path, discardLogger())
	records, err := e.Extract(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if records == nil {
		t.Fatal("expected empty non-nil slice, got nil")
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}

func TestFileExtractor_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte(`{not json`), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	e := NewFileExtractor(path, discardLogger())
	if _, err := e.Extract(context.Background()); err == nil {
		t.Fatal("expected error for invalid json")
	}
}

func TestFileExtractor_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	e := NewFileExtractor("testdata/input.json", discardLogger())
	if _, err := e.Extract(ctx); err == nil {
		t.Fatal("expected context error")
	}
}

func TestFileExtractor_Name(t *testing.T) {
	e := NewFileExtractor("x", discardLogger())
	if e.Name() != fileSource {
		t.Fatalf("expected %q, got %q", fileSource, e.Name())
	}
}

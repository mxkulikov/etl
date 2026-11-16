package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"okb/test-task/internal/models"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeExtractor struct {
	records []models.RawRecord
	err     error
	name    string
}

func (f *fakeExtractor) Name() string { return f.name }
func (f *fakeExtractor) Extract(ctx context.Context) ([]models.RawRecord, error) {
	return f.records, f.err
}

type fakeTransformer struct {
	results []models.Result
	err     error
}

func (f *fakeTransformer) Transform(ctx context.Context, in []models.RawRecord) ([]models.Result, error) {
	return f.results, f.err
}

type fakeLoader struct {
	key  string
	err  error
	last []models.Result
}

func (f *fakeLoader) Load(ctx context.Context, results []models.Result) (string, error) {
	f.last = results
	return f.key, f.err
}

type fakePublisher struct {
	msgs []models.ResultMessage
	err  error
}

func (f *fakePublisher) Publish(ctx context.Context, msg models.ResultMessage) error {
	f.msgs = append(f.msgs, msg)
	return f.err
}

func TestPipeline_HappyPath(t *testing.T) {
	ext := &fakeExtractor{
		name:    "fake",
		records: []models.RawRecord{{ID: "1"}},
	}
	tr := &fakeTransformer{results: []models.Result{{ID: "1"}}}
	ld := &fakeLoader{key: "results/2026/01/01/x.json"}
	pub := &fakePublisher{}

	p := New(ext, tr, ld, pub, discardLogger())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.msgs) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(pub.msgs))
	}
	if pub.msgs[0].S3Key != ld.key {
		t.Fatalf("s3 key mismatch: %q vs %q", pub.msgs[0].S3Key, ld.key)
	}
	if pub.msgs[0].Count != 1 {
		t.Fatalf("expected count=1, got %d", pub.msgs[0].Count)
	}
	if len(ld.last) != 1 {
		t.Fatalf("loader got %d results, want 1", len(ld.last))
	}
}

func TestPipeline_ExtractError(t *testing.T) {
	ext := &fakeExtractor{name: "fake", err: errors.New("boom")}
	p := New(ext, &fakeTransformer{}, &fakeLoader{}, &fakePublisher{}, discardLogger())

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestPipeline_TransformError(t *testing.T) {
	ext := &fakeExtractor{name: "fake", records: []models.RawRecord{{ID: "1"}}}
	tr := &fakeTransformer{err: errors.New("boom")}
	pub := &fakePublisher{}

	p := New(ext, tr, &fakeLoader{}, pub, discardLogger())
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if len(pub.msgs) != 0 {
		t.Fatal("must not publish when transform fails")
	}
}

func TestPipeline_LoadError_NoPublish(t *testing.T) {
	ext := &fakeExtractor{name: "fake", records: []models.RawRecord{{ID: "1"}}}
	tr := &fakeTransformer{results: []models.Result{{ID: "1"}}}
	ld := &fakeLoader{err: errors.New("s3 down")}
	pub := &fakePublisher{}

	p := New(ext, tr, ld, pub, discardLogger())
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if len(pub.msgs) != 0 {
		t.Fatal("must not publish when load fails")
	}
}

func TestPipeline_PublishError(t *testing.T) {
	ext := &fakeExtractor{name: "fake", records: []models.RawRecord{{ID: "1"}}}
	tr := &fakeTransformer{results: []models.Result{{ID: "1"}}}
	ld := &fakeLoader{key: "k"}
	pub := &fakePublisher{err: errors.New("kafka down")}

	p := New(ext, tr, ld, pub, discardLogger())
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

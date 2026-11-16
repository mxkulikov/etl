package extractor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"okb/test-task/internal/models"
)

const fileSource = "file"

// FileExtractor читает входные данные из JSON-файла.
// Формат: {"schema_version": 1, "records": [...]}
type FileExtractor struct {
	path string
	log  *slog.Logger
}

func NewFileExtractor(path string, log *slog.Logger) *FileExtractor {
	if log == nil {
		log = slog.Default()
	}
	return &FileExtractor{path: path, log: log}
}

func (e *FileExtractor) Name() string { return fileSource }

type filePayload struct {
	SchemaVersion int              `json:"schema_version"`
	Records       []map[string]any `json:"records"`
}

func (e *FileExtractor) Extract(ctx context.Context) ([]models.RawRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("file extractor: %w", err)
	}

	raw, err := os.ReadFile(e.path)
	if err != nil {
		return nil, fmt.Errorf("file extractor: read %q: %w", e.path, err)
	}

	var p filePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("file extractor: parse %q: %w", e.path, err)
	}

	now := time.Now().UTC()
	records := make([]models.RawRecord, 0, len(p.Records))
	var skipped int

	for _, rec := range p.Records {
		r, ok := scanFileRecord(rec, now)
		if !ok {
			skipped++
			e.log.Warn("skip record without id", "source", fileSource)
			continue
		}
		records = append(records, r)
	}

	e.log.Info("file extractor done",
		"source", fileSource,
		"total", len(p.Records),
		"accepted", len(records),
		"skipped", skipped,
		"schema_version", p.SchemaVersion,
	)

	return records, nil
}

func scanFileRecord(rec map[string]any, now time.Time) (models.RawRecord, bool) {
    id, ok := rec["id"].(string)
    if !ok || id == "" {
        return models.RawRecord{}, false
    }
    return models.RawRecord{
        ID:        id,
        Source:    fileSource,
        Payload:   rec,
        FetchedAt: now,
    }, true
}
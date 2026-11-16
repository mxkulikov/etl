package transformer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"okb/test-task/internal/models"
)

// Transformer применяет бизнес-правила к сырым записям.
// Правила:
//   - name: trim пробелов; пусто после trim -> запись пропускается;
//   - amount: приводится к float64; отрицательный -> запись пропускается;
//   - в Data добавляются поля processed_at (UTC now) и source.
type Transformer struct {
	log *slog.Logger
}

func New(log *slog.Logger) *Transformer {
	if log == nil {
		log = slog.Default()
	}
	return &Transformer{log: log}
}

func (t *Transformer) Transform(ctx context.Context, in []models.RawRecord) ([]models.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("transformer: %w", err)
	}

	now := time.Now().UTC()
	out := make([]models.Result, 0, len(in))
	var skipped int

	for _, rec := range in {
		name, _ := rec.Payload["name"].(string)
		name = strings.TrimSpace(name)
		if name == "" {
			skipped++
			t.log.Warn("skip record: empty name",
				"id", rec.ID, "source", rec.Source)
			continue
		}

		amount, ok := toFloat64(rec.Payload["amount"])
		if !ok {
			skipped++
			t.log.Warn("skip record: amount is not a number",
				"id", rec.ID, "source", rec.Source)
			continue
		}
		if amount < 0 {
			skipped++
			t.log.Warn("skip record: negative amount",
				"id", rec.ID, "source", rec.Source, "amount", amount)
			continue
		}

		data := make(map[string]any, len(rec.Payload)+2)
		for k, v := range rec.Payload {
			data[k] = v
		}
		data["name"] = name
		data["amount"] = amount
		data["processed_at"] = now
		data["source"] = rec.Source

		out = append(out, models.Result{
			ID:         rec.ID,
			Source:     rec.Source,
			Data:       data,
			ProducedAt: now,
		})
	}

	t.log.Info("transformer done",
		"in", len(in), "accepted", len(out), "skipped", skipped)

	return out, nil
}

// toFloat64 приводит значение из JSON (float64 по умолчанию) к float64.
// Дополнительно поддерживает int и json.Number — на случай, если источник
// отдаёт числа иначе (например, pgx или protojson).
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

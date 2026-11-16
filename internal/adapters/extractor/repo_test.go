package extractor

import (
	"testing"
	"time"
)

func TestScanRecord(t *testing.T) {
	now := time.Now().UTC()

	rec, ok := scanRecord("id-1", "Alice", 100.5, now)
	if !ok {
		t.Fatal("expected ok")
	}
	if rec.ID != "id-1" || rec.Source != postgresSource {
		t.Fatalf("unexpected: %+v", rec)
	}
	if rec.Payload["name"] != "Alice" || rec.Payload["amount"] != 100.5 {
		t.Fatalf("payload: %+v", rec.Payload)
	}
	if !rec.FetchedAt.Equal(now) {
		t.Fatal("fetched_at mismatch")
	}

	_, ok = scanRecord("", "Bob", 0, now)
	if ok {
		t.Fatal("expected skip for empty id")
	}
}

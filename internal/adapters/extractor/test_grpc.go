package extractor

import (
    "testing"
    "time"

    recordspb "okb/test-task/api/proto"
)

func TestScanGRPCRecord(t *testing.T) {
    now := time.Now().UTC()

    rec, ok := scanGRPCRecord(&recordspb.Record{
        Id: "rec-1", Name: "Alice", Amount: 100.5,
    }, now)
    if !ok {
        t.Fatal("expected ok")
    }
    if rec.ID != "rec-1" || rec.Source != grpcSource {
        t.Fatalf("unexpected: %+v", rec)
    }
    if rec.Payload["name"] != "Alice" || rec.Payload["amount"] != 100.5 {
        t.Fatalf("payload: %+v", rec.Payload)
    }

    _, ok = scanGRPCRecord(&recordspb.Record{Name: "Bob"}, now)
    if ok {
        t.Fatal("expected skip for empty id")
    }
}
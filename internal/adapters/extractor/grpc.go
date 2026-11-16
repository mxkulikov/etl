package extractor

import (
    "context"
    "fmt"
    "log/slog"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    recordspb "okb/test-task/api/proto"
    "okb/test-task/internal/models"
)

const grpcSource = "grpc"

type GRPCExtractor struct {
    conn    *grpc.ClientConn
    client  recordspb.RecordServiceClient
    timeout time.Duration
    log     *slog.Logger
}

func NewGRPCExtractor(target string, timeout time.Duration, log *slog.Logger) (*GRPCExtractor, error) {
    if log == nil {
        log = slog.Default()
    }
    conn, err := grpc.NewClient(target,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        return nil, fmt.Errorf("grpc extractor: dial %q: %w", target, err)
    }
    return &GRPCExtractor{
        conn:    conn,
        client:  recordspb.NewRecordServiceClient(conn),
        timeout: timeout,
        log:     log,
    }, nil
}

func (e *GRPCExtractor) Name() string { return grpcSource }

func (e *GRPCExtractor) Close() error { return e.conn.Close() }

func (e *GRPCExtractor) Extract(ctx context.Context) ([]models.RawRecord, error) {
    ctx, cancel := context.WithTimeout(ctx, e.timeout)
    defer cancel()

    resp, err := e.client.GetRecords(ctx, &recordspb.GetRecordsRequest{})
    if err != nil {
        return nil, fmt.Errorf("grpc extractor: GetRecords: %w", err)
    }

    now := time.Now().UTC()
    records := make([]models.RawRecord, 0, len(resp.GetRecords()))
    for _, r := range resp.GetRecords() {
        rec, ok := scanGRPCRecord(r, now)
        if !ok {
            e.log.Warn("skip grpc record without id", "source", grpcSource)
            continue
        }
        records = append(records, rec)
    }

    e.log.Info("grpc extractor done",
        "source", grpcSource,
        "total", len(resp.GetRecords()),
        "accepted", len(records),
    )
    return records, nil
}

// scanGRPCRecord маппит одну запись из gRPC в RawRecord.
// Вынес для тестируемости.
func scanGRPCRecord(r *recordspb.Record, now time.Time) (models.RawRecord, bool) {
    if r.GetId() == "" {
        return models.RawRecord{}, false
    }
    return models.RawRecord{
        ID:     r.GetId(),
        Source: grpcSource,
        Payload: map[string]any{
            "name":   r.GetName(),
            "amount": r.GetAmount(),
        },
        FetchedAt: now,
    }, true
}
package loader

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"okb/test-task/internal/models"
)

// S3Loader пишет результаты в S3-совместимое хранилище (в моем случае MinIO).
// Один объект на батч: results/YYYY/MM/DD/<hex>.json
type S3Loader struct {
	client *minio.Client
	bucket string
	log    *slog.Logger
}

type S3Config struct {
	Endpoint  string
	Bucket    string
	UseSSL    bool
	AccessKey string
	SecretKey string
}

func NewS3Loader(cfg S3Config, log *slog.Logger) (*S3Loader, error) {
	if log == nil {
		log = slog.Default()
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 loader: create client: %w", err)
	}
	return &S3Loader{
		client: client,
		bucket: cfg.Bucket,
		log:    log,
	}, nil
}

func (l *S3Loader) Load(ctx context.Context, results []models.Result) (string, error) {
	// 1. Сериализуем батч.
	//    Оборачиваем в объект с meta, чтобы можно было добавить поля без ломки парсера.
	body, err := json.Marshal(struct {
		ProducedAt time.Time       `json:"produced_at"`
		Count      int             `json:"count"`
		Results    []models.Result `json:"results"`
	}{
		ProducedAt: time.Now().UTC(),
		Count:      len(results),
		Results:    results,
	})
	if err != nil {
		return "", fmt.Errorf("s3 loader: marshal results: %w", err)
	}

	// 2. Генерируем ключ: results/YYYY/MM/DD/<hex>.json
	key, err := buildObjectKey(time.Now().UTC())
	if err != nil {
		return "", fmt.Errorf("s3 loader: build key: %w", err)
	}

	// 3. Загружаем. PutObject принимает io.Reader и размер.
	//    bytes.Reader реализует io.Reader; размер знаем из len(body).
	_, err = l.client.PutObject(ctx, l.bucket, key,
		bytes.NewReader(body),
		int64(len(body)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		return "", fmt.Errorf("s3 loader: put %q: %w", key, err)
	}

	l.log.Info("s3 loader done",
		"bucket", l.bucket,
		"key", key,
		"count", len(results),
		"bytes", len(body),
	)
	return key, nil
}

// buildObjectKey генерирует ключ вида results/2026/11/08/<hex>.json.
// Вынесено для тестируемости (как scanRecord в extractor).
func buildObjectKey(now time.Time) (string, error) {
	b := make([]byte, 8) // 16 hex-символов
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	return fmt.Sprintf("results/%s/%s.json",
		now.Format("2006/01/02"),
		hex.EncodeToString(b),
	), nil
}

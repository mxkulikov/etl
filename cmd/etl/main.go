package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"okb/test-task/internal/adapters/extractor"
	"okb/test-task/internal/adapters/loader"
	"okb/test-task/internal/adapters/publisher"
	"okb/test-task/internal/config"
	"okb/test-task/internal/models"
	"okb/test-task/internal/pipeline"
	"okb/test-task/internal/transformer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		return err
	}

	log := newLogger(cfg.App.LogLevel)
	slog.SetDefault(log)

	// --- Graceful shutdown ---
	// первый SIGINT/SIGTERM отменяет контекст, пайплайн завершается штатно
	// второй SIGINT/SIGTERM провоцирует принудительный выход (как docker stop / docker kill).
	// os.Interrupt == syscall.SIGINT, поэтому перечислил только уникальные сигналы.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case s := <-sigCh:
			log.Info("signal received, starting graceful shutdown", "signal", s.String())
			cancel()
		case <-rootCtx.Done():
			return
		}
		select {
		case s := <-sigCh:
			log.Warn("second signal received, forcing exit", "signal", s.String())
			os.Exit(130) // 128 + SIGINT(2) конвенция POSIX
		case <-rootCtx.Done():
			return
		}
	}()

	ctx, cancelTimeout := context.WithTimeout(rootCtx, cfg.App.RunTimeout)
	defer cancelTimeout()

	fileExt := extractor.NewFileExtractor(cfg.Input.ConfigFile, log)

	grpcExt, err := extractor.NewGRPCExtractor(cfg.GRPC.Target, cfg.GRPC.Timeout, log)
	if err != nil {
		return fmt.Errorf("init grpc extractor: %w", err)
	}
	defer grpcExt.Close()

	pgExt, err := extractor.NewPostgresExtractor(extractor.PGConfig{
		Host:         cfg.Postgres.Host,
		Port:         cfg.Postgres.Port,
		Database:     cfg.Postgres.Database,
		User:         cfg.Postgres.User,
		Password:     cfg.Postgres.Password,
		MaxConns:     cfg.Postgres.MaxConns,
		QueryTimeout: cfg.Postgres.QueryTimeout,
		Table:        cfg.Postgres.Table,
	}, log)
	if err != nil {
		return fmt.Errorf("init postgres extractor: %w", err)
	}
	defer pgExt.Close()

	s3Loader, err := loader.NewS3Loader(loader.S3Config{
		Endpoint:  cfg.Minio.Endpoint,
		Bucket:    cfg.Minio.Bucket,
		UseSSL:    cfg.Minio.UseSSL,
		AccessKey: cfg.Minio.AccessKey,
		SecretKey: cfg.Minio.SecretKey,
	}, log)
	if err != nil {
		return fmt.Errorf("init s3 loader: %w", err)
	}

	kafkaPub := publisher.NewKafkaPublisher(publisher.KafkaConfig{
		Brokers: cfg.Kafka.Brokers,
		Topic:   cfg.Kafka.Topic,
	}, log)
	defer kafkaPub.Close()

	tr := transformer.New(log)

	// Последовательный прогон: ошибка одного источника останавливает пайплайн (fail fast).
	// Параллельная обработка через errgroup — TODO.
	extractors := []models.Extractor{fileExt, grpcExt, pgExt}

	for _, ext := range extractors {
		log.Info("pipeline starting",
			"source", ext.Name(),
			"timeout", cfg.App.RunTimeout.String(),
		)

		p := pipeline.New(ext, tr, s3Loader, kafkaPub, log)
		if err := p.Run(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("pipeline cancelled by signal", "source", ext.Name())
				return nil
			}
			return fmt.Errorf("pipeline for %s: %w", ext.Name(), err)
		}
	}

	log.Info("all pipelines finished successfully")
	return nil
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

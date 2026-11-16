package config

import (
	"fmt"
	"log/slog"
	"os"
	"time"
	"errors"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App      AppConfig      `yaml:"app"`
	Postgres PostgresConfig `yaml:"postgres"`
	GRPC     GRPCConfig     `yaml:"grpc"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Minio    MinioConfig    `yaml:"minio"`
	Input    InputConfig    `yaml:"input"`
}

type AppConfig struct {
	LogLevel   string        `yaml:"log_level"`
	RunTimeout time.Duration `yaml:"run_timeout"`
}

type PostgresConfig struct {
	Host         string        `yaml:"host"`
	Port         int           `yaml:"port"`
	Database     string        `yaml:"database"`
	User         string        `yaml:"user"`
	Password     string        `yaml:"-"` // даю понять, что пароль беру только из .ENV
	MaxConns     int           `yaml:"max_conns"`
	QueryTimeout time.Duration `yaml:"query_timeout"`
	Table        string        `yaml:"table"`
}

type GRPCConfig struct {
	Target  string        `yaml:"target"`
	Timeout time.Duration `yaml:"timeout"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

type MinioConfig struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	UseSSL    bool   `yaml:"use_ssl"`
	AccessKey string `yaml:"-"`
	SecretKey string `yaml:"-"`
}

type InputConfig struct {
	ConfigFile string `yaml:"config_file"`
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	applyEnvOverrides(&cfg)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return &cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	cfg.Postgres.Password = envOr("ETL_PG_PASSWORD", "etl_dev_password")
	cfg.Minio.AccessKey = envOr("ETL_MINIO_ACCESS_KEY", "minioadmin")
	cfg.Minio.SecretKey = envOr("ETL_MINIO_SECRET_KEY", "minioadmin")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	slog.Warn("env not set, using dev default", "env", key)
	return fallback
}

func (c *Config) Validate() error {
	if c.App.RunTimeout <= 0 {
		return fmt.Errorf("app.run_timeout must be > 0, got %s", c.App.RunTimeout)
	}
	if c.Postgres.Host == "" {
		return errors.New("postgres.host is required")
	}
	if c.Postgres.Database == "" {
		return errors.New("postgres.database is required")
	}
	if c.Postgres.Table == "" {
		return errors.New("postgres.table is required")
	}
	if c.GRPC.Target == "" {
		return errors.New("grpc.target is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("kafka.brokers must contain at least one broker")
	}
	if c.Kafka.Topic == "" {
		return errors.New("kafka.topic is required")
	}
	if c.Minio.Endpoint == "" {
		return errors.New("minio.endpoint is required")
	}
	if c.Minio.Bucket == "" {
		return errors.New("minio.bucket is required")
	}
	if c.Input.ConfigFile == "" {
		return errors.New("input.config_file is required")
	}
	return nil
}

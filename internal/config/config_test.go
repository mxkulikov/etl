package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(`
		app: { run_timeout: 1m }
		postgres: { host: h, database: d, table: t }
		grpc: { target: "x:1" }
		kafka: { brokers: ["b:1"], topic: "t" }
		minio: { endpoint: "m:1", bucket: "b" }
		input: { config_file: "f.json" }
	`), 0o644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if cfg.Postgres.Password == "" {
		t.Fatal("expected dev default password")
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(`app: { run_timeout: 1m }`), 0o644)

	if _, err := Load(path); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidate_RejectsNegativeTimeout(t *testing.T) {
	c := &Config{}
	c.App.RunTimeout = -1
	if err := c.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

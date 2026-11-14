package config_test

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"echpochmak/internal/config"
	"echpochmak/internal/logger"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(nil, env(nil))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.HTTP.Addr != ":8181" || cfg.Log.Level != slog.LevelInfo || cfg.Log.Format != logger.FormatJSON {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}

	if cfg.Store.Backend != config.StoreMemory {
		t.Fatalf("Store.Backend = %q, want %q", cfg.Store.Backend, config.StoreMemory)
	}
}

func TestLoadPrecedence(t *testing.T) {
	t.Parallel()

	vars := map[string]string{
		"ECHPOCHMAK_HTTP_ADDR":         ":9000",
		"ECHPOCHMAK_LOG_LEVEL":         "debug",
		"ECHPOCHMAK_HTTP_READ_TIMEOUT": "3s",
		"ECHPOCHMAK_S3_PATH_STYLE":     "true",
	}

	cfg, err := config.Load([]string{"-http-addr", ":7000"}, env(vars))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.HTTP.Addr != ":7000" {
		t.Errorf("HTTP.Addr = %q, want the flag value :7000", cfg.HTTP.Addr)
	}

	if cfg.Log.Level != slog.LevelDebug || cfg.HTTP.ReadTimeout != 3*time.Second || !cfg.S3.PathStyle {
		t.Errorf("environment not applied: %+v", cfg)
	}
}

func TestLoadErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		vars map[string]string
		want string
	}{
		{name: "unknown flag", args: []string{"-nope"}, want: "nope"},
		{name: "help", args: []string{"-help"}, want: flag.ErrHelp.Error()},
		{name: "bad env value", vars: map[string]string{"ECHPOCHMAK_LOG_FORMAT": "xml"}, want: "ECHPOCHMAK_LOG_FORMAT"},
		{name: "non-positive timeout", args: []string{"-http-idle-timeout", "0s"}, want: "http-idle-timeout"},
		{name: "unsupported backend", args: []string{"-store-backend", "etcd"}, want: "store-backend"},
		{name: "raft without endpoints", args: []string{"-store-backend", "raft"}, want: "kv-endpoints"},
		{
			name: "budgets do not nest",
			args: []string{"-store-backend", "raft", "-kv-endpoints", "a:2379", "-http-write-timeout", "10s"},
			want: "worst-case commit",
		},
		{
			name: "attempt longer than request",
			args: []string{"-store-backend", "raft", "-kv-endpoints", "a:2379", "-kv-attempt-timeout", "9s"},
			want: "kv-attempt-timeout",
		},
		{name: "bad warehouse", args: []string{"-s3-warehouse", "/tmp/w"}, want: "s3-warehouse"},
		{name: "bad catalog name", args: []string{"-catalog-name", "a/b"}, want: "catalog-name"},
		{name: "negative retries", args: []string{"-commit-retries", "-1"}, want: "commit-retries"},
		{name: "negative cache size", args: []string{"-metadata-cache-size", "-1"}, want: "metadata-cache-size"},
		{name: "zero body limit", args: []string{"-http-max-body-bytes", "0"}, want: "http-max-body-bytes"},
		{name: "half credentials", args: []string{"-s3-access-key-id", "key"}, want: "set together"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := config.Load(tt.args, env(tt.vars))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want one mentioning %q", err, tt.want)
			}
		})
	}
}

func TestHelpIsErrHelp(t *testing.T) {
	t.Parallel()

	_, err := config.Load([]string{"-h"}, env(nil))
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("Load(-h) error = %v, want flag.ErrHelp", err)
	}
}

func TestSecretIsRedacted(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(
		[]string{"-s3-access-key-id", "AKIA", "-s3-secret-access-key", "hunter2"},
		env(nil),
	)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.S3.SecretAccessKey != "hunter2" {
		t.Fatalf("SecretAccessKey not loaded")
	}

	var buf bytes.Buffer

	slog.New(slog.NewJSONHandler(&buf, nil)).Info("config", slog.Any("config", cfg))
	printed := cfg.S3.SecretAccessKey.String() + fmt.Sprintf("%v", cfg)

	for _, out := range []string{buf.String(), printed} {
		if strings.Contains(out, "hunter2") {
			t.Fatalf("secret leaked: %s", out)
		}
	}
}

func TestRaftBackend(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(nil, env(map[string]string{
		"ECHPOCHMAK_STORE_BACKEND": "raft",
		"ECHPOCHMAK_KV_ENDPOINTS":  "kv1:2379, kv2:2379,kv3:2379",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(cfg.Store.KVEndpoints) != 3 || cfg.Store.KVEndpoints[1] != "kv2:2379" {
		t.Fatalf("KVEndpoints = %q", cfg.Store.KVEndpoints)
	}
}

func TestWarehouseSchemes(t *testing.T) {
	t.Parallel()

	for _, w := range []string{"s3://w/", "s3a://w/", "file:///tmp/w", "mem://w/"} {
		if _, err := config.Load([]string{"-s3-warehouse", w}, env(nil)); err != nil {
			t.Errorf("warehouse %q rejected: %v", w, err)
		}
	}
}

func TestS3IOProps(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load([]string{
		"-s3-endpoint", "http://minio:9000",
		"-s3-path-style",
		"-s3-access-key-id", "key",
		"-s3-secret-access-key", "secret",
	}, env(nil))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	props := cfg.S3.IOProps()
	want := map[string]string{
		"s3.endpoint":                 "http://minio:9000",
		"s3.region":                   "us-east-1",
		"s3.force-virtual-addressing": "false",
		"s3.access-key-id":            "key",
		"s3.secret-access-key":        "secret",
	}

	for k, v := range want {
		if props[k] != v {
			t.Errorf("IOProps()[%q] = %q, want %q", k, props[k], v)
		}
	}
}

func TestEnvName(t *testing.T) {
	t.Parallel()

	if got := config.EnvName("s3-secret-access-key"); got != "ECHPOCHMAK_S3_SECRET_ACCESS_KEY" {
		t.Fatalf("EnvName = %q", got)
	}
}

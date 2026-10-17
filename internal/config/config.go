// Package config loads the catalog-server configuration from command-line
// flags and environment variables.
//
// Every flag has an environment variable named after it: the flag name in
// upper case, with dashes replaced by underscores, prefixed with [EnvPrefix].
// For example, -http-addr can be set with ECHPOCHMAK_HTTP_ADDR. A flag given
// on the command line wins over its environment variable.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"echpochmak/internal/logger"
)

// EnvPrefix prefixes the environment variable of every flag.
const EnvPrefix = "ECHPOCHMAK_"

// Store backends.
const (
	StoreMemory = "memory"
)

// Config is the catalog-server configuration.
type Config struct {
	HTTP  HTTP
	Log   logger.Config
	Store Store
	S3    S3
}

// HTTP configures the HTTP server.
type HTTP struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	// ShutdownTimeout bounds how long in-flight requests may take to finish
	// after a shutdown signal.
	ShutdownTimeout time.Duration
}

// Store selects and configures the store backend.
type Store struct {
	Backend string
}

// S3 configures access to the object storage that holds metadata files.
type S3 struct {
	Endpoint        string
	Region          string
	Warehouse       string
	AccessKeyID     string
	SecretAccessKey Secret
	PathStyle       bool
}

// Secret is a string that is never printed or logged.
type Secret string

const redacted = "[REDACTED]"

// String implements [fmt.Stringer].
func (s Secret) String() string {
	if s == "" {
		return ""
	}

	return redacted
}

// LogValue implements [slog.LogValuer].
func (s Secret) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

// Load parses args (without the program name) and falls back to environment
// variables, read with getenv, for flags not given on the command line.
func Load(args []string, getenv func(string) string) (Config, error) {
	var cfg Config

	fs := flag.NewFlagSet("catalog-server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	register(fs, &cfg)

	if err := fs.Parse(args); err != nil {
		return Config{}, fmt.Errorf("parsing flags: %w", err)
	}

	if err := applyEnv(fs, getenv); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Usage returns the help text of all flags.
func Usage() string {
	var (
		cfg Config
		b   strings.Builder
	)

	fs := flag.NewFlagSet("catalog-server", flag.ContinueOnError)
	register(fs, &cfg)
	fs.SetOutput(&b)
	fs.PrintDefaults()

	return b.String()
}

func register(fs *flag.FlagSet, cfg *Config) {
	fs.StringVar(&cfg.HTTP.Addr, "http-addr", ":8181", "HTTP listen address")
	fs.DurationVar(
		&cfg.HTTP.ReadHeaderTimeout,
		"http-read-header-timeout",
		5*time.Second,
		"time to read request headers",
	)
	fs.DurationVar(&cfg.HTTP.ReadTimeout, "http-read-timeout", 30*time.Second, "time to read a whole request")
	fs.DurationVar(&cfg.HTTP.WriteTimeout, "http-write-timeout", 60*time.Second, "time to write a response")
	fs.DurationVar(&cfg.HTTP.IdleTimeout, "http-idle-timeout", 120*time.Second, "keep-alive idle timeout")
	fs.DurationVar(&cfg.HTTP.ShutdownTimeout, "http-shutdown-timeout", 20*time.Second, "graceful shutdown timeout")

	fs.TextVar(&cfg.Log.Level, "log-level", slog.LevelInfo, "log level: debug, info, warn, error")
	fs.TextVar(&cfg.Log.Format, "log-format", logger.FormatJSON, "log format: json, text")
	fs.BoolVar(&cfg.Log.AddSource, "log-add-source", false, "add source file and line to log records")

	fs.StringVar(&cfg.Store.Backend, "store-backend", StoreMemory, "store backend: memory")

	fs.StringVar(&cfg.S3.Endpoint, "s3-endpoint", "", "S3 endpoint URL; empty for AWS")
	fs.StringVar(&cfg.S3.Region, "s3-region", "us-east-1", "S3 region")
	fs.StringVar(&cfg.S3.Warehouse, "s3-warehouse", "s3://warehouse/", "warehouse location for new tables")
	fs.StringVar(&cfg.S3.AccessKeyID, "s3-access-key-id", "", "S3 access key ID")
	fs.Func("s3-secret-access-key", "S3 secret access key", func(v string) error {
		cfg.S3.SecretAccessKey = Secret(v)

		return nil
	})
	fs.BoolVar(&cfg.S3.PathStyle, "s3-path-style", false, "use path-style S3 URLs (required by MinIO)")
}

// applyEnv sets every flag that was not given on the command line from its
// environment variable, if that is set.
func applyEnv(fs *flag.FlagSet, getenv func(string) string) error {
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })

	var errs []error

	fs.VisitAll(func(f *flag.Flag) {
		if given[f.Name] {
			return
		}

		name := EnvName(f.Name)

		value := getenv(name)
		if value == "" {
			return
		}

		if err := f.Value.Set(value); err != nil {
			errs = append(errs, fmt.Errorf("environment variable %s: %w", name, err))
		}
	})

	return errors.Join(errs...)
}

// EnvName returns the environment variable that backs the flag named flagName.
func EnvName(flagName string) string {
	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// Validate reports every invalid setting of cfg.
func (cfg Config) Validate() error {
	var errs []error

	if cfg.HTTP.Addr == "" {
		errs = append(errs, errors.New("http-addr must not be empty"))
	}

	timeouts := map[string]time.Duration{
		"http-read-header-timeout": cfg.HTTP.ReadHeaderTimeout,
		"http-read-timeout":        cfg.HTTP.ReadTimeout,
		"http-write-timeout":       cfg.HTTP.WriteTimeout,
		"http-idle-timeout":        cfg.HTTP.IdleTimeout,
		"http-shutdown-timeout":    cfg.HTTP.ShutdownTimeout,
	}
	for name, d := range timeouts {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %s", name, d))
		}
	}

	if cfg.Store.Backend != StoreMemory {
		errs = append(errs, fmt.Errorf("store-backend %q is not supported, want %q", cfg.Store.Backend, StoreMemory))
	}

	if !strings.HasPrefix(cfg.S3.Warehouse, "s3://") {
		errs = append(errs, fmt.Errorf("s3-warehouse %q must be an s3:// URL", cfg.S3.Warehouse))
	}

	if (cfg.S3.AccessKeyID == "") != (cfg.S3.SecretAccessKey == "") {
		errs = append(errs, errors.New("s3-access-key-id and s3-secret-access-key must be set together"))
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	return nil
}

// LogValue implements [slog.LogValuer]. Secrets are redacted.
func (cfg Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Group("http",
			slog.String("addr", cfg.HTTP.Addr),
			slog.Duration("read_header_timeout", cfg.HTTP.ReadHeaderTimeout),
			slog.Duration("read_timeout", cfg.HTTP.ReadTimeout),
			slog.Duration("write_timeout", cfg.HTTP.WriteTimeout),
			slog.Duration("idle_timeout", cfg.HTTP.IdleTimeout),
			slog.Duration("shutdown_timeout", cfg.HTTP.ShutdownTimeout),
		),
		slog.Group("log",
			slog.String("min_level", cfg.Log.Level.String()),
			slog.String("format", string(cfg.Log.Format)),
			slog.Bool("add_source", cfg.Log.AddSource),
		),
		slog.Group("store",
			slog.String("backend", cfg.Store.Backend),
		),
		slog.Group("s3",
			slog.String("endpoint", cfg.S3.Endpoint),
			slog.String("region", cfg.S3.Region),
			slog.String("warehouse", cfg.S3.Warehouse),
			slog.String("access_key_id", cfg.S3.AccessKeyID),
			slog.Any("secret_access_key", cfg.S3.SecretAccessKey),
			slog.Bool("path_style", cfg.S3.PathStyle),
		),
	)
}

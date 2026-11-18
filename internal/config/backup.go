package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"echpochmak/internal/backup"
	"echpochmak/internal/logger"
)

// BackupEnvPrefix prefixes the environment variable of every kv-backup flag.
const BackupEnvPrefix = "ECHPOCHMAK_BACKUP_"

// Backup is the kv-backup configuration.
type Backup struct {
	// Endpoints are the client endpoints of the KV cluster.
	Endpoints []string
	// Location is where backups are stored, such as s3://backups/kv.
	Location string
	// Interval is the pause between backups, jittered.
	Interval time.Duration
	// Timeout bounds one backup, from the snapshot to the manifest.
	Timeout time.Duration
	// KeepLast and KeepDaily are the retention policy.
	KeepLast  int
	KeepDaily int
	// VerifyInterval is the pause between test restores of the newest
	// backup; zero turns them off.
	VerifyInterval time.Duration
	// ListenMetrics is the address of /metrics and /healthz for "run".
	ListenMetrics string
	S3            S3
	Log           logger.Config
}

// LoadBackup parses kv-backup flags (without the program name and the
// command) and falls back to environment variables for flags not given. It
// returns the positional arguments too.
func LoadBackup(args []string, getenv func(string) string) (Backup, []string, error) {
	var cfg Backup

	fs := flag.NewFlagSet("kv-backup", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	registerBackup(fs, &cfg)

	if err := fs.Parse(args); err != nil {
		return Backup{}, nil, fmt.Errorf("parsing flags: %w", err)
	}

	if err := applyEnv(fs, BackupEnvPrefix, getenv); err != nil {
		return Backup{}, nil, err
	}

	if err := cfg.Validate(); err != nil {
		return Backup{}, nil, err
	}

	return cfg, fs.Args(), nil
}

// BackupUsage returns the help text of all kv-backup flags.
func BackupUsage() string {
	var cfg Backup

	return usage("kv-backup", func(fs *flag.FlagSet) { registerBackup(fs, &cfg) })
}

func registerBackup(fs *flag.FlagSet, cfg *Backup) {
	fs.Func("endpoints", "KV cluster client endpoints, comma-separated", func(v string) error {
		cfg.Endpoints = splitList(v)

		return nil
	})
	fs.StringVar(&cfg.Location, "location", "", "where backups are stored: s3://bucket/prefix, or file:// or mem://")
	fs.DurationVar(&cfg.Interval, "interval", 5*time.Minute, "pause between backups, jittered")
	fs.DurationVar(&cfg.Timeout, "timeout", 10*time.Minute, "timeout of one backup")
	fs.IntVar(&cfg.KeepLast, "keep-last", backup.DefaultKeepLast, "newest backups that retention keeps")
	fs.IntVar(&cfg.KeepDaily, "keep-daily", backup.DefaultKeepDaily,
		"days for which retention keeps the newest backup of the day")
	fs.DurationVar(&cfg.VerifyInterval, "verify-interval", 24*time.Hour,
		"pause between test restores of the newest backup; 0 turns them off")
	fs.StringVar(&cfg.ListenMetrics, "listen-metrics", ":2382", "listen address of /metrics and /healthz")
	registerS3Access(fs, &cfg.S3)

	fs.TextVar(&cfg.Log.Level, "log-level", slog.LevelInfo, "log level: debug, info, warn, error")
	fs.TextVar(&cfg.Log.Format, "log-format", logger.FormatJSON, "log format: json, text")
	fs.BoolVar(&cfg.Log.AddSource, "log-add-source", false, "add source file and line to log records")
}

// Validate reports every invalid setting.
func (c Backup) Validate() error {
	var errs []error

	if len(c.Endpoints) == 0 {
		errs = append(errs, errors.New("endpoints is required"))
	}

	if !strings.Contains(c.Location, "://") {
		errs = append(errs, fmt.Errorf("location %q must be a URL such as s3://bucket/prefix", c.Location))
	}

	if c.Interval <= 0 || c.Timeout <= 0 || c.VerifyInterval < 0 {
		errs = append(errs, errors.New("interval and timeout must be positive, verify-interval not negative"))
	}

	if c.KeepLast < 1 || c.KeepDaily < 0 {
		errs = append(errs, errors.New("keep-last must be at least 1 and keep-daily not negative"))
	}

	if (c.S3.AccessKeyID == "") != (c.S3.SecretAccessKey == "") {
		errs = append(errs, errors.New("s3-access-key-id and s3-secret-access-key must be set together"))
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	return nil
}

// LogValue implements [slog.LogValuer]. Secrets are redacted.
func (c Backup) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Any("endpoints", c.Endpoints),
		slog.String("location", c.Location),
		slog.Duration("interval", c.Interval),
		slog.Duration("timeout", c.Timeout),
		slog.Int("keep_last", c.KeepLast),
		slog.Int("keep_daily", c.KeepDaily),
		slog.Duration("verify_interval", c.VerifyInterval),
		slog.String("listen_metrics", c.ListenMetrics),
		slog.String("s3_endpoint", c.S3.Endpoint),
		slog.String("s3_region", c.S3.Region),
		slog.Bool("s3_path_style", c.S3.PathStyle),
		slog.String("s3_access_key_id", c.S3.AccessKeyID),
		slog.Any("s3_secret_access_key", c.S3.SecretAccessKey),
	)
}

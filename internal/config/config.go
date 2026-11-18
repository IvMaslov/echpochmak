// Package config loads the configuration of the catalog-server and kv-node
// binaries from command-line flags and environment variables.
//
// Every flag has an environment variable named after it: the flag name in
// upper case, with dashes replaced by underscores, prefixed with [EnvPrefix]
// for catalog-server and [KVEnvPrefix] for kv-node. For example, -http-addr
// can be set with ECHPOCHMAK_HTTP_ADDR. A flag given on the command line wins
// over its environment variable.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"echpochmak/internal/logger"
)

// EnvPrefix prefixes the environment variable of every flag.
const EnvPrefix = "ECHPOCHMAK_"

// Store backends.
const (
	StoreMemory = "memory"
	StoreRaft   = "raft"
)

// warehouseSchemes returns the URL schemes accepted by -s3-warehouse. file://
// and mem:// are for local runs without object storage and for tests.
func warehouseSchemes() []string {
	return []string{"s3://", "s3a://", "file://", "mem://"}
}

// Config is the catalog-server configuration.
type Config struct {
	HTTP    HTTP
	Log     logger.Config
	Catalog Catalog
	Store   Store
	S3      S3
}

// Catalog configures the Iceberg catalog behaviour.
type Catalog struct {
	// Name is the catalog prefix returned by /v1/config; clients put it in
	// every request path.
	Name string
	// CommitRetries is how many times a commit is retried on the server after
	// a concurrent change, as long as its requirements still hold.
	CommitRetries int
	// MetadataCacheSize is the number of metadata files kept in memory; zero
	// disables the cache.
	MetadataCacheSize int
	// IdempotencyLifetime is how long an Idempotency-Key is honoured; zero
	// disables idempotency support.
	IdempotencyLifetime time.Duration
	// PurgeWorkers is how many purge tasks this replica runs at once; zero
	// turns its purge worker off, leaving the tasks to other replicas.
	PurgeWorkers int
	// PurgeLease is how long a purge worker's claim on a task lasts.
	PurgeLease time.Duration
	// PurgeInterval is how often an idle purge worker scans the queue.
	PurgeInterval time.Duration
	// OrphanCleanup runs the janitor that deletes the metadata files of
	// commits that never landed. Turn it off after a restore until the
	// restore report is done.
	OrphanCleanup bool
	// OrphanInterval is the pause between janitor sweeps.
	OrphanInterval time.Duration
	// OrphanGrace is how old a commit intent must be before the janitor
	// condemns it; zero means twice the HTTP write timeout.
	OrphanGrace time.Duration
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
	// MaxBodyBytes limits the size of request bodies.
	MaxBodyBytes int64
	// DrainDelay is how long a stopping server keeps serving while it reports
	// itself unready, so a load balancer moves new requests elsewhere first.
	DrainDelay time.Duration
	// Docs serves the Swagger UI and the served spec.
	Docs bool
	// Metrics serves Prometheus metrics at /metrics.
	Metrics bool
}

// Store selects and configures the store backend.
type Store struct {
	Backend string
	// KVEndpoints are the client endpoints of the KV cluster (raft backend).
	KVEndpoints []string
	// KVAttemptTimeout bounds one attempt of the KV client on one node.
	KVAttemptTimeout time.Duration
	// KVRequestTimeout bounds one store call, the client's retries included.
	KVRequestTimeout time.Duration
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

// IOProps returns the iceberg-go file IO properties for this configuration.
func (s S3) IOProps() map[string]string {
	props := map[string]string{
		"s3.region":                   s.Region,
		"s3.force-virtual-addressing": strconv.FormatBool(!s.PathStyle),
	}

	if s.Endpoint != "" {
		props["s3.endpoint"] = s.Endpoint
	}

	if s.AccessKeyID != "" {
		props["s3.access-key-id"] = s.AccessKeyID
		props["s3.secret-access-key"] = string(s.SecretAccessKey)
	}

	return props
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
	cfg, _, err := LoadWithArgs(args, getenv)

	return cfg, err
}

// LoadWithArgs is [Load] for tools that share the catalog-server flags and
// take a command after them: it also returns the arguments after the flags.
func LoadWithArgs(args []string, getenv func(string) string) (Config, []string, error) {
	var cfg Config

	fs := flag.NewFlagSet("catalog-server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	register(fs, &cfg)

	if err := fs.Parse(args); err != nil {
		return Config{}, nil, fmt.Errorf("parsing flags: %w", err)
	}

	if err := applyEnv(fs, EnvPrefix, getenv); err != nil {
		return Config{}, nil, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, nil, err
	}

	return cfg, fs.Args(), nil
}

// Usage returns the help text of all catalog-server flags.
func Usage() string {
	var cfg Config

	return usage("catalog-server", func(fs *flag.FlagSet) { register(fs, &cfg) })
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
	fs.DurationVar(&cfg.HTTP.WriteTimeout, "http-write-timeout", 90*time.Second, "time to write a response")
	fs.DurationVar(&cfg.HTTP.IdleTimeout, "http-idle-timeout", 120*time.Second, "keep-alive idle timeout")
	fs.DurationVar(&cfg.HTTP.ShutdownTimeout, "http-shutdown-timeout", 20*time.Second, "graceful shutdown timeout")
	fs.Int64Var(&cfg.HTTP.MaxBodyBytes, "http-max-body-bytes", 16<<20, "maximum request body size in bytes")
	fs.DurationVar(&cfg.HTTP.DrainDelay, "http-drain-delay", 0,
		"on shutdown, report unready for this long before draining; set it to the load balancer's check interval")

	fs.TextVar(&cfg.Log.Level, "log-level", slog.LevelInfo, "log level: debug, info, warn, error")
	fs.TextVar(&cfg.Log.Format, "log-format", logger.FormatJSON, "log format: json, text")
	fs.BoolVar(&cfg.Log.AddSource, "log-add-source", false, "add source file and line to log records")

	fs.StringVar(&cfg.Catalog.Name, "catalog-name", "main", "catalog name, used as the URL prefix by clients")
	fs.IntVar(&cfg.Catalog.CommitRetries, "commit-retries", 4, "server-side retries of a conflicting commit")
	fs.IntVar(
		&cfg.Catalog.MetadataCacheSize,
		"metadata-cache-size",
		1000,
		"metadata files cached in memory; 0 disables",
	)

	fs.DurationVar(&cfg.Catalog.IdempotencyLifetime, "idempotency-lifetime", 30*time.Minute,
		"how long an Idempotency-Key is honoured; 0 disables idempotency support")
	fs.IntVar(&cfg.Catalog.PurgeWorkers, "purge-workers", 2,
		"purge tasks run at once by this replica; 0 leaves them to other replicas")
	fs.DurationVar(&cfg.Catalog.PurgeLease, "purge-lease", 2*time.Minute,
		"how long a purge worker's claim on a task lasts; renewed between delete batches")
	fs.DurationVar(&cfg.Catalog.PurgeInterval, "purge-interval", 10*time.Second,
		"how often an idle purge worker scans the queue, jittered")
	fs.BoolVar(&cfg.Catalog.OrphanCleanup, "orphan-cleanup", true,
		"delete the metadata files of commits that never landed; turn off after a restore until the report is done")
	fs.DurationVar(&cfg.Catalog.OrphanInterval, "orphan-interval", time.Minute,
		"pause between sweeps of the orphan janitor, jittered")
	fs.DurationVar(&cfg.Catalog.OrphanGrace, "orphan-grace", 0,
		"age at which a commit intent counts as abandoned; 0 means twice http-write-timeout")
	fs.BoolVar(&cfg.HTTP.Docs, "docs", true, "serve the Swagger UI at /docs and the spec at /openapi.json")
	fs.BoolVar(&cfg.HTTP.Metrics, "metrics", true, "serve Prometheus metrics at /metrics")

	fs.StringVar(&cfg.Store.Backend, "store-backend", StoreMemory, "store backend: memory or raft")
	fs.Func("kv-endpoints", "KV cluster client endpoints, comma-separated (raft backend)", func(v string) error {
		cfg.Store.KVEndpoints = splitList(v)

		return nil
	})
	fs.DurationVar(&cfg.Store.KVAttemptTimeout, "kv-attempt-timeout", 2*time.Second,
		"timeout of one KV client attempt on one node")
	fs.DurationVar(&cfg.Store.KVRequestTimeout, "kv-request-timeout", 5*time.Second,
		"timeout of one store call, KV client retries included")

	registerS3Access(fs, &cfg.S3)
	fs.StringVar(
		&cfg.S3.Warehouse,
		"s3-warehouse",
		"s3://warehouse/",
		"warehouse location for new tables: s3://, s3a://, file:// or mem:// URL",
	)
}

// registerS3Access registers the flags that reach S3: endpoint, region,
// credentials and addressing.
func registerS3Access(fs *flag.FlagSet, s3 *S3) {
	fs.StringVar(&s3.Endpoint, "s3-endpoint", "", "S3 endpoint URL; empty for AWS")
	fs.StringVar(&s3.Region, "s3-region", "us-east-1", "S3 region")
	fs.StringVar(&s3.AccessKeyID, "s3-access-key-id", "", "S3 access key ID")
	fs.Func("s3-secret-access-key", "S3 secret access key", func(v string) error {
		s3.SecretAccessKey = Secret(v)

		return nil
	})
	fs.BoolVar(&s3.PathStyle, "s3-path-style", false, "use path-style S3 URLs (required by MinIO)")
}

// applyEnv sets every flag that was not given on the command line from its
// environment variable, named with prefix, if that is set.
func applyEnv(fs *flag.FlagSet, prefix string, getenv func(string) string) error {
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })

	var errs []error

	fs.VisitAll(func(f *flag.Flag) {
		if given[f.Name] {
			return
		}

		name := envName(prefix, f.Name)

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

// EnvName returns the environment variable that backs the catalog-server
// flag named flagName.
func EnvName(flagName string) string {
	return envName(EnvPrefix, flagName)
}

func envName(prefix, flagName string) string {
	return prefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// usage returns the help text of the flags registered by register.
func usage(name string, register func(*flag.FlagSet)) string {
	var b strings.Builder

	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	register(fs)
	fs.SetOutput(&b)
	fs.PrintDefaults()

	return b.String()
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

	if cfg.HTTP.MaxBodyBytes <= 0 {
		errs = append(errs, fmt.Errorf("http-max-body-bytes must be positive, got %d", cfg.HTTP.MaxBodyBytes))
	}

	errs = append(errs, cfg.Catalog.validate()...)

	errs = append(errs, cfg.validateStore()...)

	schemes := warehouseSchemes()
	if !slices.ContainsFunc(schemes, func(scheme string) bool {
		return strings.HasPrefix(cfg.S3.Warehouse, scheme)
	}) {
		errs = append(errs, fmt.Errorf(
			"s3-warehouse %q must start with one of %s",
			cfg.S3.Warehouse,
			strings.Join(schemes, ", "),
		))
	}

	if (cfg.S3.AccessKeyID == "") != (cfg.S3.SecretAccessKey == "") {
		errs = append(errs, errors.New("s3-access-key-id and s3-secret-access-key must be set together"))
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	return nil
}

// commitStoreCalls is how many store calls one commit attempt makes: read the
// pointer, record the commit intent, then swap the pointer.
const commitStoreCalls = 3

func (cfg Config) validateStore() []error {
	var errs []error

	switch cfg.Store.Backend {
	case StoreMemory:
		return nil
	case StoreRaft:
	default:
		return []error{fmt.Errorf("store-backend %q is not supported, want %q or %q",
			cfg.Store.Backend, StoreMemory, StoreRaft)}
	}

	if len(cfg.Store.KVEndpoints) == 0 {
		errs = append(errs, errors.New("kv-endpoints is required for the raft backend"))
	}

	if cfg.Store.KVAttemptTimeout <= 0 || cfg.Store.KVRequestTimeout < cfg.Store.KVAttemptTimeout {
		errs = append(errs, fmt.Errorf("need 0 < kv-attempt-timeout (%s) <= kv-request-timeout (%s)",
			cfg.Store.KVAttemptTimeout, cfg.Store.KVRequestTimeout))
	}

	// The budgets must nest, or a commit the server is still retrying is
	// reported to the client as a failure when the HTTP response times out.
	worstCommit := time.Duration((cfg.Catalog.CommitRetries+1)*commitStoreCalls) * cfg.Store.KVRequestTimeout
	if cfg.HTTP.WriteTimeout <= worstCommit {
		errs = append(errs, fmt.Errorf(
			"http-write-timeout (%s) must exceed the worst-case commit, "+
				"(commit-retries+1) x %d x kv-request-timeout = %s",
			cfg.HTTP.WriteTimeout, commitStoreCalls, worstCommit))
	}

	return errs
}

func (c Catalog) validate() []error {
	var errs []error

	if c.Name == "" || strings.ContainsAny(c.Name, "/?#%") {
		errs = append(errs, fmt.Errorf("catalog-name %q must be non-empty and URL path safe", c.Name))
	}

	if c.CommitRetries < 0 {
		errs = append(errs, fmt.Errorf("commit-retries must not be negative, got %d", c.CommitRetries))
	}

	if c.IdempotencyLifetime < 0 {
		errs = append(errs, fmt.Errorf("idempotency-lifetime must not be negative, got %s", c.IdempotencyLifetime))
	}

	if c.MetadataCacheSize < 0 {
		errs = append(errs, fmt.Errorf("metadata-cache-size must not be negative, got %d", c.MetadataCacheSize))
	}

	if c.PurgeWorkers < 0 {
		errs = append(errs, fmt.Errorf("purge-workers must not be negative, got %d", c.PurgeWorkers))
	}

	if c.PurgeLease <= 0 || c.PurgeInterval <= 0 {
		errs = append(errs, fmt.Errorf("purge-lease and purge-interval must be positive, got %s and %s",
			c.PurgeLease, c.PurgeInterval))
	}

	if c.OrphanInterval <= 0 || c.OrphanGrace < 0 {
		errs = append(errs, fmt.Errorf("orphan-interval must be positive and orphan-grace not negative, got %s and %s",
			c.OrphanInterval, c.OrphanGrace))
	}

	return errs
}

// OrphanGrace returns the age at which the janitor condemns a commit intent:
// the configured grace, or twice the HTTP write timeout, beyond which no
// request is still running.
func (cfg Config) OrphanGrace() time.Duration {
	if cfg.Catalog.OrphanGrace > 0 {
		return cfg.Catalog.OrphanGrace
	}

	return 2 * cfg.HTTP.WriteTimeout
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
			slog.Int64("max_body_bytes", cfg.HTTP.MaxBodyBytes),
			slog.Duration("drain_delay", cfg.HTTP.DrainDelay),
			slog.Bool("docs", cfg.HTTP.Docs),
			slog.Bool("metrics", cfg.HTTP.Metrics),
		),
		slog.Group("catalog",
			slog.String("name", cfg.Catalog.Name),
			slog.Int("commit_retries", cfg.Catalog.CommitRetries),
			slog.Int("metadata_cache_size", cfg.Catalog.MetadataCacheSize),
			slog.Duration("idempotency_lifetime", cfg.Catalog.IdempotencyLifetime),
			slog.Int("purge_workers", cfg.Catalog.PurgeWorkers),
			slog.Duration("purge_lease", cfg.Catalog.PurgeLease),
			slog.Duration("purge_interval", cfg.Catalog.PurgeInterval),
			slog.Bool("orphan_cleanup", cfg.Catalog.OrphanCleanup),
			slog.Duration("orphan_interval", cfg.Catalog.OrphanInterval),
			slog.Duration("orphan_grace", cfg.OrphanGrace()),
		),
		slog.Group("log",
			slog.String("min_level", cfg.Log.Level.String()),
			slog.String("format", string(cfg.Log.Format)),
			slog.Bool("add_source", cfg.Log.AddSource),
		),
		slog.Group("store",
			slog.String("backend", cfg.Store.Backend),
			slog.Any("kv_endpoints", cfg.Store.KVEndpoints),
			slog.Duration("kv_attempt_timeout", cfg.Store.KVAttemptTimeout),
			slog.Duration("kv_request_timeout", cfg.Store.KVRequestTimeout),
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

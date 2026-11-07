// Package logger builds the application [slog.Logger] and carries
// request-scoped loggers through a [context.Context].
//
// Only main calls [slog.SetDefault]. Other packages receive a *slog.Logger
// explicitly or take it from the request context with [FromContext].
package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// Attribute keys shared across the application. Use them instead of string
// literals so the same thing is always logged under the same key.
const (
	KeyService   = "service"
	KeyInstance  = "instance"
	KeyBuild     = "build"
	KeyError     = "error"
	KeyRequestID = "request_id"
	KeyMethod    = "method"
	KeyRoute     = "route"
	KeyPath      = "path"
	KeyStatus    = "status"
	KeyDuration  = "duration"
	KeyBytes     = "bytes"
	KeyRemote    = "remote"
	KeyAddr      = "addr"
	KeyConfig    = "config"
	KeySignal    = "signal"
	KeyPanic     = "panic"
	KeyNamespace = "namespace"
	KeyTable     = "table"
	KeyView      = "view"
	KeyLocation  = "location"
	KeyVersion   = "version"
	KeyAttempt   = "attempt"
	KeyReason    = "reason"
)

// Format selects the output encoding.
type Format string

// Supported formats.
const (
	// FormatJSON writes one JSON object per line; use it in deployments.
	FormatJSON Format = "json"
	// FormatText writes key=value pairs; use it for local development.
	FormatText Format = "text"
)

// UnmarshalText implements [encoding.TextUnmarshaler], so a Format can be
// used with [flag.TextVar].
func (f *Format) UnmarshalText(text []byte) error {
	switch Format(text) {
	case FormatJSON, FormatText:
		*f = Format(text)

		return nil
	default:
		return fmt.Errorf("unknown log format %q, want %q or %q", text, FormatJSON, FormatText)
	}
}

// MarshalText implements [encoding.TextMarshaler].
func (f Format) MarshalText() ([]byte, error) {
	return []byte(f), nil
}

// Config configures the logger.
type Config struct {
	Level     slog.Level
	Format    Format
	AddSource bool
}

// New returns a logger that writes to w and tags every record with the
// service name and the instance (host or pod) name.
func New(w io.Writer, cfg Config, service string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     cfg.Level,
		AddSource: cfg.AddSource,
	}

	var handler slog.Handler = slog.NewJSONHandler(w, opts)
	if cfg.Format == FormatText {
		handler = slog.NewTextHandler(w, opts)
	}

	instance, err := os.Hostname()
	if err != nil {
		instance = "unknown"
	}

	return slog.New(handler).With(
		slog.String(KeyService, service),
		slog.String(KeyInstance, instance),
	)
}

// Err returns the attribute for an error.
func Err(err error) slog.Attr {
	return slog.Any(KeyError, err)
}

type ctxKey struct{}

// WithContext returns a copy of ctx that carries l.
func WithContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext returns the logger carried by ctx, or [slog.Default] if there is
// none. HTTP handlers get a logger tagged with the request ID this way.
func FromContext(ctx context.Context) *slog.Logger {
	return FromContextOr(ctx, slog.Default())
}

// FromContextOr returns the logger carried by ctx, or fallback if there is
// none.
func FromContextOr(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}

	return fallback
}

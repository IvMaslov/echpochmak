package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"echpochmak/internal/logger"
)

func TestNewFiltersByLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	l := logger.New(&buf, logger.Config{Level: slog.LevelWarn, Format: logger.FormatJSON}, "test")
	l.Info("hidden")
	l.Warn("shown")

	out := buf.String()
	if strings.Contains(out, "hidden") || !strings.Contains(out, "shown") {
		t.Fatalf("output %q: want only the warning", out)
	}
}

func TestNewAddsBaseAttributes(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger.New(&buf, logger.Config{Format: logger.FormatJSON}, "catalog").Info("hello")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, buf.String())
	}

	if rec[logger.KeyService] != "catalog" {
		t.Errorf("%s = %v, want catalog", logger.KeyService, rec[logger.KeyService])
	}

	for _, key := range []string{logger.KeyInstance} {
		if _, ok := rec[key]; !ok {
			t.Errorf("record has no %q attribute: %v", key, rec)
		}
	}
}

func TestNewTextFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger.New(&buf, logger.Config{Format: logger.FormatText}, "catalog").Info("hello")

	if !strings.Contains(buf.String(), "service=catalog") {
		t.Fatalf("output %q is not in text format", buf.String())
	}
}

func TestFormatUnmarshalText(t *testing.T) {
	t.Parallel()

	var f logger.Format
	if err := f.UnmarshalText([]byte("text")); err != nil || f != logger.FormatText {
		t.Fatalf("UnmarshalText(text) = %v, %q", err, f)
	}

	if err := f.UnmarshalText([]byte("xml")); err == nil {
		t.Fatal("UnmarshalText(xml) succeeded, want error")
	}
}

func TestContextRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	l := slog.New(slog.NewTextHandler(&buf, nil)).With(slog.String(logger.KeyRequestID, "abc"))
	ctx := logger.WithContext(t.Context(), l)

	logger.FromContext(ctx).Info("hello")

	if !strings.Contains(buf.String(), "request_id=abc") {
		t.Fatalf("output %q: want the logger stored in the context", buf.String())
	}

	if logger.FromContext(context.Background()) != slog.Default() {
		t.Fatal("FromContext without a logger: want slog.Default()")
	}
}

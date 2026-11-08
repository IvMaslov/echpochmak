package node

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

// raftLogger adapts slog to the logger interface of the raft library. Raft's
// info messages (elections, term changes) are useful but frequent; they are
// logged at debug level, and the node logs leader changes itself.
type raftLogger struct {
	log *slog.Logger
}

func (l raftLogger) logf(level slog.Level, format string, v ...any) {
	l.log.Log(context.Background(), level, fmt.Sprintf(format, v...))
}

func (l raftLogger) Debug(v ...any)                 { l.logf(slog.LevelDebug, "%s", fmt.Sprint(v...)) }
func (l raftLogger) Debugf(format string, v ...any) { l.logf(slog.LevelDebug, format, v...) }
func (l raftLogger) Info(v ...any)                  { l.logf(slog.LevelDebug, "%s", fmt.Sprint(v...)) }
func (l raftLogger) Infof(format string, v ...any)  { l.logf(slog.LevelDebug, format, v...) }
func (l raftLogger) Warning(v ...any)               { l.logf(slog.LevelWarn, "%s", fmt.Sprint(v...)) }

func (l raftLogger) Warningf(format string, v ...any) { l.logf(slog.LevelWarn, format, v...) }
func (l raftLogger) Error(v ...any)                   { l.logf(slog.LevelError, "%s", fmt.Sprint(v...)) }
func (l raftLogger) Errorf(format string, v ...any)   { l.logf(slog.LevelError, format, v...) }

// Fatal logs and exits, as the raft library expects.
func (l raftLogger) Fatal(v ...any) {
	l.logf(slog.LevelError, "%s", fmt.Sprint(v...))
	os.Exit(1)
}

// Fatalf logs and exits, as the raft library expects.
func (l raftLogger) Fatalf(format string, v ...any) {
	l.logf(slog.LevelError, format, v...)
	os.Exit(1)
}

// Panic logs and panics, as the raft library expects.
func (l raftLogger) Panic(v ...any) {
	msg := fmt.Sprint(v...)
	l.logf(slog.LevelError, "%s", msg)
	panic(msg)
}

// Panicf logs and panics, as the raft library expects.
func (l raftLogger) Panicf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	l.logf(slog.LevelError, "%s", msg)
	panic(msg)
}

// Package version holds build information, injected at link time:
//
//	go build -ldflags "-X echpochmak/internal/version.version=v1.2.3 ..."
//
// When a value is not injected, it falls back to the VCS information that the
// Go toolchain embeds in the binary.
package version

import (
	"log/slog"
	"runtime/debug"
)

// Set via -ldflags; see the package documentation.
var (
	version   = ""
	commit    = ""
	buildDate = ""
)

// Info describes the running build.
type Info struct {
	Version   string
	Commit    string
	BuildDate string
}

// Get returns the build information of the running binary.
func Get() Info {
	info := Info{
		Version:   version,
		Commit:    commit,
		BuildDate: buildDate,
	}

	if bi, ok := debug.ReadBuildInfo(); ok {
		if info.Version == "" && bi.Main.Version != "(devel)" {
			info.Version = bi.Main.Version
		}

		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if info.Commit == "" {
					info.Commit = s.Value
				}
			case "vcs.time":
				if info.BuildDate == "" {
					info.BuildDate = s.Value
				}
			}
		}
	}

	if info.Version == "" {
		info.Version = "dev"
	}

	return info
}

// LogValue implements [slog.LogValuer].
func (i Info) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("version", i.Version),
		slog.String("commit", i.Commit),
		slog.String("build_date", i.BuildDate),
	)
}

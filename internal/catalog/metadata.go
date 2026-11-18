package catalog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/klauspost/compress/zstd"

	"echpochmak/internal/fileio"
	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// Magic numbers of the compression formats Iceberg uses for metadata files.
const (
	gzipMagic = "\x1f\x8b"
	zstdMagic = "\x28\xb5\x2f\xfd"
)

// Metadata file names: "00012-<uuid>.metadata.json", optionally with a codec
// before ".metadata.json", or the legacy "v12.metadata.json".
var (
	metadataFileName       = regexp.MustCompile(`^(\d+)-[0-9a-fA-F-]{36}(?:\.\w+)?\.metadata\.json$`)
	legacyMetadataFileName = regexp.MustCompile(`^v(\d+)(?:\.\w+)?\.metadata\.json$`)
)

// readMetadata reads and parses the metadata file at location. It returns the
// parsed metadata and its uncompressed JSON.
func (s *Service) readMetadata(ctx context.Context, location string) (table.Metadata, []byte, error) {
	raw, err := s.readMetadataJSON(ctx, location)
	if err != nil {
		return nil, nil, err
	}

	md, err := table.ParseMetadataBytes(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing metadata file %q: %w", location, err)
	}

	return md, raw, nil
}

// readMetadataJSON returns the uncompressed JSON of the metadata file at
// location.
func (s *Service) readMetadataJSON(ctx context.Context, location string) ([]byte, error) {
	data, err := s.files.Read(ctx, location)
	if errors.Is(err, fileio.ErrNotFound) {
		return nil, fmt.Errorf("%w: %q", ErrMetadataMissing, location)
	}

	if err != nil {
		return nil, fmt.Errorf("reading metadata file: %w", err)
	}

	raw, err := decompress(data)
	if err != nil {
		return nil, fmt.Errorf("decompressing metadata file %q: %w", location, err)
	}

	return raw, nil
}

// writeMetadata encodes md with the codec its properties ask for, writes it to
// location and returns the uncompressed JSON.
func (s *Service) writeMetadata(ctx context.Context, location string, md table.Metadata) ([]byte, error) {
	raw, err := json.Marshal(md)
	if err != nil {
		return nil, fmt.Errorf("encoding metadata: %w", err)
	}

	codec := md.Properties().Get(table.MetadataCompressionKey, table.MetadataCompressionDefault)

	data, err := compress(raw, codec)
	if err != nil {
		return nil, err
	}

	if err := s.files.Write(ctx, location, data); err != nil {
		return nil, fmt.Errorf("writing metadata file: %w", err)
	}

	return raw, nil
}

// deleteFile removes a file the catalog no longer needs and reports whether
// it is gone. Failures only leave an orphan behind, so they are logged and
// otherwise ignored. The deletion runs even if the request was cancelled.
func (s *Service) deleteFile(ctx context.Context, location, reason string) bool {
	ctx = context.WithoutCancel(ctx)

	if err := s.files.Delete(ctx, location); err != nil {
		s.metrics.deleteFailed()
		s.logFor(ctx).WarnContext(ctx, "deleting metadata file failed",
			logger.Err(err),
			slog.String(logger.KeyLocation, location),
			slog.String(logger.KeyReason, reason),
		)

		return false
	}

	return true
}

// newMetadataLocation returns the location of the next metadata file of md,
// following the current file at currentLocation ("" for a new table).
func newMetadataLocation(md, current table.Metadata, currentLocation string) (string, error) {
	version := 0
	if currentLocation != "" {
		version = nextMetadataVersion(current, currentLocation)
	}

	provider, err := table.LoadLocationProvider(md.Location(), md.Properties())
	if err != nil {
		return "", fmt.Errorf("%w: table location %q: %w", ErrInvalidRequest, md.Location(), err)
	}

	loc, err := provider.NewTableMetadataFileLocation(version)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}

	return loc, nil
}

// nextMetadataVersion returns the version number for the file after the one
// at currentLocation. Names that do not follow the Iceberg convention, as in
// registered tables, fall back to the length of the metadata log.
func nextMetadataVersion(current table.Metadata, currentLocation string) int {
	name := path.Base(currentLocation)

	for _, re := range []*regexp.Regexp{metadataFileName, legacyMetadataFileName} {
		if m := re.FindStringSubmatch(name); m != nil {
			if v, err := strconv.Atoi(m[1]); err == nil {
				return v + 1
			}
		}
	}

	return len(slices.Collect(current.PreviousFiles())) + 1
}

// defaultTableLocation returns the location of a new table without an
// explicit one. The table UUID in the last segment keeps a recreated table
// out of the directory of a dropped one, whose files are not purged.
func (s *Service) defaultTableLocation(ns store.Namespace, nsProps map[string]string, name, tableUUID string) string {
	if nsLocation := strings.TrimSuffix(nsProps["location"], "/"); nsLocation != "" {
		return nsLocation + "/" + name
	}

	return s.warehouse + "/" + strings.Join(ns, "/") + "/" + name + "-" + tableUUID
}

// removedMetadataFiles returns the files in the metadata log of before that
// are no longer in the log of after.
func removedMetadataFiles(before, after table.Metadata) []string {
	if before == nil {
		return nil
	}

	kept := map[string]bool{}
	for entry := range after.PreviousFiles() {
		kept[entry.MetadataFile] = true
	}

	removed := []string{}

	for entry := range before.PreviousFiles() {
		if !kept[entry.MetadataFile] {
			removed = append(removed, entry.MetadataFile)
		}
	}

	return removed
}

func compress(raw []byte, codec string) ([]byte, error) {
	switch codec {
	case table.MetadataCompressionCodecNone:
		return raw, nil
	case table.MetadataCompressionCodecGzip:
		var buf bytes.Buffer

		w := gzip.NewWriter(&buf)
		if _, err := w.Write(raw); err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}

		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}

		return buf.Bytes(), nil
	case table.MetadataCompressionCodecZstd:
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		defer enc.Close()

		return enc.EncodeAll(raw, nil), nil
	default:
		return nil, fmt.Errorf("%w: unsupported metadata compression codec %q", ErrInvalidRequest, codec)
	}
}

func decompress(data []byte) ([]byte, error) {
	switch {
	case bytes.HasPrefix(data, []byte(gzipMagic)):
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		defer r.Close()

		raw, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}

		return raw, nil
	case bytes.HasPrefix(data, []byte(zstdMagic)):
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		defer dec.Close()

		raw, err := dec.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}

		return raw, nil
	default:
		return data, nil
	}
}

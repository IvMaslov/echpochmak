// Package snapshot reads and writes snapshot files: a consistent copy of a
// KV cluster's replicated state that can be stored anywhere and restored
// into a new cluster without a running node.
//
// A file is:
//
//	magic       "ECHKVSNP"
//	version     uint32, big-endian; currently 1
//	header      uvarint length, then a protobuf kvpb.SnapshotHeader
//	body        the state's record stream, zstd-compressed
//	trailer     uint64 big-endian size of the uncompressed body, then the
//	            SHA-256 of every byte before the trailer
//
// The body is what [statemachine.Export.WriteTo] produces and
// [statemachine.StateMachine.ImportSnapshot] reads. A reader checks the
// trailer when it reaches the end, so a truncated or corrupt file is always
// reported, and [Verify] checks a whole file before anything is restored.
package snapshot

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// Magic starts every snapshot file.
const Magic = "ECHKVSNP"

// FormatVersion is the version of the format this package writes. Readers
// reject other versions.
const FormatVersion = 1

const (
	trailerSize = 8 + sha256.Size
	// maxHeaderBytes bounds the header, so a corrupt length cannot make the
	// reader allocate without limit.
	maxHeaderBytes = 1 << 20
)

// ErrCorrupt means a file is not a valid snapshot: wrong magic, unknown
// version, truncated, or failing its checksum.
var ErrCorrupt = errors.New("corrupt snapshot file")

// Writer writes a snapshot file. Write the body with Write, then Close.
type Writer struct {
	dst   io.Writer
	hash  hash.Hash
	zw    *zstd.Encoder
	size  uint64
	ended bool
}

// NewWriter writes the magic, the version and h to w, and returns a Writer
// for the body.
func NewWriter(w io.Writer, h *kvpb.SnapshotHeader) (*Writer, error) {
	raw, err := proto.Marshal(h)
	if err != nil {
		return nil, fmt.Errorf("encoding snapshot header: %w", err)
	}

	sw := &Writer{dst: w, hash: sha256.New()}
	hashed := io.MultiWriter(w, sw.hash)

	prefix := make([]byte, 0, len(Magic)+4+binary.MaxVarintLen64+len(raw))
	prefix = append(prefix, Magic...)
	prefix = binary.BigEndian.AppendUint32(prefix, FormatVersion)
	prefix = binary.AppendUvarint(prefix, uint64(len(raw)))
	prefix = append(prefix, raw...)

	if _, err := hashed.Write(prefix); err != nil {
		return nil, fmt.Errorf("writing snapshot header: %w", err)
	}

	zw, err := zstd.NewWriter(hashed, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("starting compression: %w", err)
	}

	sw.zw = zw

	return sw, nil
}

// Write adds p to the body.
func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.zw.Write(p)
	w.size += uint64(n) //nolint:gosec // G115: n is never negative.

	if err != nil {
		return n, fmt.Errorf("compressing snapshot: %w", err)
	}

	return n, nil
}

// Close ends the body and writes the trailer. It does not close the
// underlying writer.
func (w *Writer) Close() error {
	if w.ended {
		return nil
	}

	w.ended = true

	if err := w.zw.Close(); err != nil {
		return fmt.Errorf("ending compression: %w", err)
	}

	trailer := binary.BigEndian.AppendUint64(make([]byte, 0, trailerSize), w.size)
	trailer = w.hash.Sum(trailer)

	if _, err := w.dst.Write(trailer); err != nil {
		return fmt.Errorf("writing snapshot trailer: %w", err)
	}

	return nil
}

// Reader reads a snapshot file.
type Reader struct {
	header *kvpb.SnapshotHeader
	tail   *tailReader
	zr     *zstd.Decoder
	size   uint64
	sum    []byte
	err    error
}

// NewReader reads the magic, the version and the header from r, and returns
// a Reader for the body. Close it when done.
func NewReader(r io.Reader) (*Reader, error) {
	tr := newTailReader(r, trailerSize)
	br := bufio.NewReader(tr)

	prefix := make([]byte, len(Magic)+4)
	if _, err := io.ReadFull(br, prefix); err != nil {
		return nil, fmt.Errorf("%w: reading the magic: %w", ErrCorrupt, err)
	}

	if string(prefix[:len(Magic)]) != Magic {
		return nil, fmt.Errorf("%w: not a snapshot file", ErrCorrupt)
	}

	if v := binary.BigEndian.Uint32(prefix[len(Magic):]); v != FormatVersion {
		return nil, fmt.Errorf("%w: format version %d, want %d", ErrCorrupt, v, FormatVersion)
	}

	n, err := binary.ReadUvarint(br)
	if err != nil || n > maxHeaderBytes {
		return nil, fmt.Errorf("%w: bad header length", ErrCorrupt)
	}

	raw := make([]byte, n)
	if _, err := io.ReadFull(br, raw); err != nil {
		return nil, fmt.Errorf("%w: reading the header: %w", ErrCorrupt, err)
	}

	h := &kvpb.SnapshotHeader{}
	if err := proto.Unmarshal(raw, h); err != nil {
		return nil, fmt.Errorf("%w: decoding the header: %w", ErrCorrupt, err)
	}

	zr, err := zstd.NewReader(br, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("starting decompression: %w", err)
	}

	return &Reader{header: h, tail: tr, zr: zr}, nil
}

// Header returns the snapshot's header.
func (r *Reader) Header() *kvpb.SnapshotHeader {
	return proto.CloneOf(r.header)
}

// Read reads the decompressed body. At the end of the body it checks the
// trailer, and returns [ErrCorrupt] instead of io.EOF if it does not match.
func (r *Reader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	n, err := r.zr.Read(p)
	r.size += uint64(n) //nolint:gosec // G115: n is never negative.

	switch {
	case errors.Is(err, io.EOF):
		r.err = r.check()

		return n, r.err
	case err != nil:
		r.err = fmt.Errorf("%w: decompressing: %w", ErrCorrupt, err)

		return n, r.err
	default:
		return n, nil
	}
}

// check compares the trailer with what was read. The body is fully read, so
// the tail reader has seen every byte before the trailer.
func (r *Reader) check() error {
	// Drain what the decompressor did not need; the trailer must follow.
	if _, err := io.Copy(io.Discard, r.tail); err != nil {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}

	trailer, sum, ok := r.tail.finish()
	if !ok {
		return fmt.Errorf("%w: truncated", ErrCorrupt)
	}

	if size := binary.BigEndian.Uint64(trailer[:8]); size != r.size {
		return fmt.Errorf("%w: body is %d bytes, the trailer says %d", ErrCorrupt, r.size, size)
	}

	if !bytes.Equal(trailer[8:], sum) {
		return fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	r.sum = sum

	return io.EOF
}

// Checksum returns the hex SHA-256 recorded in the trailer, once the body was
// read to its end and found valid; empty before.
func (r *Reader) Checksum() string {
	return hex.EncodeToString(r.sum)
}

// Close releases the decompressor.
func (r *Reader) Close() {
	r.zr.Close()
}

// Info describes a valid snapshot file.
type Info struct {
	Header *kvpb.SnapshotHeader
	// Checksum is the hex SHA-256 from the trailer. It identifies the
	// snapshot: restores salt the new cluster ID with it.
	Checksum string
	// BodySize is the size of the uncompressed body.
	BodySize uint64
}

// Verify reads a whole snapshot file and describes it if it is valid.
func Verify(r io.Reader) (Info, error) {
	sr, err := NewReader(r)
	if err != nil {
		return Info{}, err
	}
	defer sr.Close()

	if _, err := io.Copy(io.Discard, sr); err != nil {
		return Info{}, fmt.Errorf("reading the snapshot: %w", err)
	}

	return Info{Header: sr.Header(), Checksum: sr.Checksum(), BodySize: sr.size}, nil
}

// tailReader passes through every byte of r except the last n, which it
// keeps, and hashes what it passes through.
type tailReader struct {
	r    io.Reader
	n    int
	buf  []byte
	hash hash.Hash
	eof  bool
}

func newTailReader(r io.Reader, n int) *tailReader {
	return &tailReader{r: r, n: n, buf: make([]byte, 0, 64<<10), hash: sha256.New()}
}

func (t *tailReader) Read(p []byte) (int, error) {
	for !t.eof && len(t.buf) <= t.n {
		if err := t.fill(); err != nil {
			return 0, err
		}
	}

	avail := len(t.buf) - t.n
	if avail <= 0 {
		return 0, io.EOF
	}

	n := copy(p, t.buf[:avail])
	t.hash.Write(t.buf[:n])
	t.buf = append(t.buf[:0], t.buf[n:]...)

	return n, nil
}

// fill reads more of r into buf.
func (t *tailReader) fill() error {
	if len(t.buf) == cap(t.buf) {
		t.buf = append(t.buf, make([]byte, cap(t.buf))...)[:len(t.buf)]
	}

	n, err := t.r.Read(t.buf[len(t.buf):cap(t.buf)])
	t.buf = t.buf[:len(t.buf)+n]

	switch {
	case errors.Is(err, io.EOF):
		t.eof = true

		return nil
	case err != nil:
		return fmt.Errorf("reading snapshot: %w", err)
	default:
		return nil
	}
}

// finish returns the kept tail and the hash of everything before it, once r
// is exhausted; ok is false if fewer than n bytes were kept.
func (t *tailReader) finish() (tail, sum []byte, ok bool) {
	if !t.eof || len(t.buf) != t.n {
		return nil, nil, false
	}

	return t.buf, t.hash.Sum(nil), true
}

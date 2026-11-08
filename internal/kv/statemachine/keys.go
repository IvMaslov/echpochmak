package statemachine

import "encoding/binary"

// Key layout of the replicated state inside the node's Pebble database. Every
// key starts with prefixState, so a snapshot is the whole prefixState range.
const (
	prefixState      = "s/"
	prefixData       = "s/data/"
	prefixDedup      = "s/dedup/"
	prefixDedupByRev = "s/dedupidx/"
	keyApplied       = "s/meta/applied"
)

// rangeAllFromKey is the range end that means "every key from key on".
const rangeAllFromKey = "\x00"

// dataKey returns the database key of a user key.
func dataKey(key []byte) []byte {
	return append([]byte(prefixData), key...)
}

// dataRange returns the database bounds [lower, upper) of the user range
// [key, rangeEnd), with the conventions of RangeRequest: an empty rangeEnd is
// the single key, and "\0" is every key from key on.
func dataRange(key, rangeEnd []byte) (lower, upper []byte) {
	lower = dataKey(key)

	switch string(rangeEnd) {
	case "":
		return lower, append(dataKey(key), 0)
	case rangeAllFromKey:
		return lower, prefixEnd([]byte(prefixData))
	default:
		return lower, dataKey(rangeEnd)
	}
}

// prefixEnd returns the smallest key greater than every key with prefix p.
func prefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++

			return end[:i+1]
		}
	}

	// All 0xff: there is no upper bound; callers never pass such prefixes.
	return nil
}

func dedupKey(requestID []byte) []byte {
	return append([]byte(prefixDedup), requestID...)
}

// dedupIndexKey orders dedup records by revision, for pruning.
func dedupIndexKey(revision int64, requestID []byte) []byte {
	k := make([]byte, 0, len(prefixDedupByRev)+8+len(requestID))
	k = append(k, prefixDedupByRev...)
	k = binary.BigEndian.AppendUint64(k, uint64(revision)) //nolint:gosec // G115: revisions are never negative.

	return append(k, requestID...)
}

// parseDedupIndexKey returns the revision and request ID of a dedup index key.
func parseDedupIndexKey(k []byte) (revision int64, requestID []byte) {
	rest := k[len(prefixDedupByRev):]

	return int64(binary.BigEndian.Uint64(rest[:8])), rest[8:] //nolint:gosec // G115: written from a non-negative int64.
}

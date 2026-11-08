package kvtest

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// KV is the store under a linearizability test: reads, blind writes and
// compare-and-swap on single keys.
type KV interface {
	// Get returns the value and mod revision of key, or found=false.
	Get(ctx context.Context, key string) (value string, modRevision int64, found bool, err error)
	// Put writes key and returns the revision of the write.
	Put(ctx context.Context, key, value string) (revision int64, err error)
	// CAS writes key if its mod revision is expected (0: absent) and
	// returns whether it did, and the revision of the write.
	CAS(ctx context.Context, key string, expected int64, value string) (swapped bool, revision int64, err error)
}

// Workload drives a linearizability test.
type Workload struct {
	KV KV
	// Unknown reports whether a failed write may have been applied. Writes
	// that failed for sure are left out of the history; unknown ones are
	// kept as possibly applied.
	Unknown  func(error) bool
	Clients  int
	Keys     int
	Duration time.Duration
	Seed     uint64
	// Timeout bounds each operation.
	Timeout time.Duration
}

type opKind int

const (
	opGet opKind = iota
	opPut
	opCAS
)

type lzInput struct {
	kind     opKind
	key      string
	value    string
	expected int64
}

type lzOutput struct {
	unknown  bool
	found    bool
	value    string
	revision int64
	swapped  bool
}

// lzState is one key's register. A revision of -1 means the key holds the
// value of a write whose revision was never observed.
type lzState struct {
	found    bool
	value    string
	revision int64
}

const unknownRevision = -1

// Run runs the workload until its duration passes and returns the history.
func (w Workload) Run(ctx context.Context) []porcupine.Operation {
	start := time.Now()
	deadline := start.Add(w.Duration)

	var (
		mu      sync.Mutex
		history []porcupine.Operation
		wg      sync.WaitGroup
	)

	for client := range w.Clients {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(w.Seed, uint64(client))) //nolint:gosec // G404, G115: test workload.
			lastSeen := map[string]int64{}

			for i := 0; time.Now().Before(deadline) && ctx.Err() == nil; i++ {
				in := lzInput{key: "k" + strconv.Itoa(rng.IntN(w.Keys))}

				switch rng.IntN(3) {
				case 0:
					in.kind = opGet
				case 1:
					in.kind, in.value = opPut, fmt.Sprintf("c%d-%d", client, i)
				default:
					in.kind, in.value, in.expected = opCAS, fmt.Sprintf("c%d-%d", client, i), lastSeen[in.key]
				}

				call := time.Since(start).Nanoseconds()
				out, keep := w.do(ctx, in, lastSeen)
				ret := time.Since(start).Nanoseconds()

				if !keep {
					continue
				}

				if out.unknown {
					// The write may take effect at any later time.
					ret = math.MaxInt64
				}

				mu.Lock()
				history = append(history, porcupine.Operation{
					ClientId: client,
					Input:    in,
					Call:     call,
					Output:   out,
					Return:   ret,
				})
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	return history
}

// do runs one operation. keep is false for operations that certainly had no
// effect, which are left out of the history.
func (w Workload) do(ctx context.Context, in lzInput, lastSeen map[string]int64) (lzOutput, bool) {
	ctx, cancel := context.WithTimeout(ctx, w.Timeout)
	defer cancel()

	var (
		out lzOutput
		err error
	)

	switch in.kind {
	case opGet:
		out.value, out.revision, out.found, err = w.KV.Get(ctx, in.key)
		if err != nil {
			return out, false
		}

		lastSeen[in.key] = out.revision

		return out, true
	case opPut:
		out.revision, err = w.KV.Put(ctx, in.key, in.value)
	case opCAS:
		out.swapped, out.revision, err = w.KV.CAS(ctx, in.key, in.expected, in.value)
	}

	switch {
	case err == nil:
		if in.kind == opPut || out.swapped {
			lastSeen[in.key] = out.revision
		}

		return out, true
	case w.Unknown(err):
		return lzOutput{unknown: true}, true
	default:
		return out, false
	}
}

// Model is the porcupine model of a store of independent registers, one per
// key.
func Model() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := map[string][]porcupine.Operation{}
			for _, op := range history {
				key := as[lzInput](op.Input).key
				byKey[key] = append(byKey[key], op)
			}

			parts := make([][]porcupine.Operation, 0, len(byKey))
			for _, k := range slices.Sorted(maps.Keys(byKey)) {
				parts = append(parts, byKey[k])
			}

			return parts
		},
		Init: func() []any { return []any{lzState{}} },
		Step: func(state, input, output any) []any {
			return step(as[lzState](state), as[lzInput](input), as[lzOutput](output))
		},
		DescribeOperation: func(input, output any) string {
			in, out := as[lzInput](input), as[lzOutput](output)

			return fmt.Sprintf("%v %+v -> %+v", in.kind, in, out)
		},
	}

	return nm.ToModel()
}

// step returns every state the register can be in after the operation, or
// none if the operation's output is impossible in state s.
func step(s lzState, in lzInput, out lzOutput) []any {
	switch in.kind {
	case opGet:
		if out.found != s.found || (out.found && out.value != s.value) {
			return nil
		}

		if s.revision == unknownRevision {
			// The read reveals the revision of the unobserved write.
			s.revision = out.revision

			return []any{s}
		}

		if out.revision != s.revision {
			return nil
		}

		return []any{s}
	case opPut:
		written := lzState{found: true, value: in.value, revision: out.revision}

		if out.unknown {
			written.revision = unknownRevision

			return []any{s, written}
		}

		if !newer(out.revision, s) {
			return nil
		}

		return []any{written}
	default:
		return stepCAS(s, in, out)
	}
}

func stepCAS(s lzState, in lzInput, out lzOutput) []any {
	// With an unobserved revision, the compare may have gone either way.
	matches := s.revision == unknownRevision || s.revision == in.expected
	differs := s.revision == unknownRevision || s.revision != in.expected

	switch {
	case out.unknown:
		states := []any{s}
		if matches {
			states = append(states, lzState{found: true, value: in.value, revision: unknownRevision})
		}

		return states
	case out.swapped:
		if !matches || !newer(out.revision, s) {
			return nil
		}

		return []any{lzState{found: true, value: in.value, revision: out.revision}}
	default:
		if !differs {
			return nil
		}

		return []any{s}
	}
}

// as converts a value the model put into porcupine back to its type. The
// model only ever sees its own types, so a mismatch is a bug in the test.
func as[T any](v any) T {
	t, ok := v.(T)
	if !ok {
		panic(fmt.Sprintf("linearizability model: unexpected %T", v))
	}

	return t
}

// newer reports whether a write at revision could follow state s: revisions
// only grow.
func newer(revision int64, s lzState) bool {
	return s.revision == unknownRevision || revision > s.revision
}

func (k opKind) String() string {
	switch k {
	case opGet:
		return "get"
	case opPut:
		return "put"
	default:
		return "cas"
	}
}

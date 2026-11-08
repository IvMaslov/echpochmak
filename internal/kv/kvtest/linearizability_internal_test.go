package kvtest

import (
	"math"
	"testing"

	"github.com/anishathalye/porcupine"
)

func op(client int, call, ret int64, in lzInput, out lzOutput) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret}
}

// The helpers use one key: the model treats keys independently.
const key = "k"

func put(value string) lzInput { return lzInput{kind: opPut, key: key, value: value} }
func get() lzInput             { return lzInput{kind: opGet, key: key} }

func cas(expected int64, value string) lzInput {
	return lzInput{kind: opCAS, key: key, expected: expected, value: value}
}

func TestModelAcceptsAndRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		history []porcupine.Operation
		ok      bool
	}{
		{
			name: "read after write",
			history: []porcupine.Operation{
				op(0, 0, 10, put("a"), lzOutput{revision: 1}),
				op(1, 20, 30, get(), lzOutput{found: true, value: "a", revision: 1}),
			},
			ok: true,
		},
		{
			name: "stale read after a completed write",
			history: []porcupine.Operation{
				op(0, 0, 10, put("a"), lzOutput{revision: 1}),
				op(0, 20, 30, put("b"), lzOutput{revision: 2}),
				op(1, 40, 50, get(), lzOutput{found: true, value: "a", revision: 1}),
			},
			ok: false,
		},
		{
			name: "two compare-and-swaps on one revision both succeed",
			history: []porcupine.Operation{
				op(0, 0, 10, put("a"), lzOutput{revision: 1}),
				op(1, 20, 30, cas(1, "b"), lzOutput{swapped: true, revision: 2}),
				op(2, 20, 30, cas(1, "c"), lzOutput{swapped: true, revision: 3}),
			},
			ok: false,
		},
		{
			name: "revision goes backwards",
			history: []porcupine.Operation{
				op(0, 0, 10, put("a"), lzOutput{revision: 5}),
				op(0, 20, 30, put("b"), lzOutput{revision: 4}),
			},
			ok: false,
		},
		{
			name: "unknown write that later shows up",
			history: []porcupine.Operation{
				op(0, 0, math.MaxInt64, put("a"), lzOutput{unknown: true}),
				op(1, 20, 30, get(), lzOutput{found: true, value: "a", revision: 7}),
				op(1, 40, 50, get(), lzOutput{found: true, value: "a", revision: 7}),
			},
			ok: true,
		},
		{
			name: "unknown write that never happened",
			history: []porcupine.Operation{
				op(0, 0, math.MaxInt64, put("a"), lzOutput{unknown: true}),
				op(1, 20, 30, get(), lzOutput{}),
			},
			ok: true,
		},
		{
			name: "read of a value nobody wrote",
			history: []porcupine.Operation{
				op(1, 20, 30, get(), lzOutput{found: true, value: "ghost", revision: 1}),
			},
			ok: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := porcupine.CheckOperations(Model(), tt.history); got != tt.ok {
				t.Fatalf("linearizable = %v, want %v", got, tt.ok)
			}
		})
	}
}

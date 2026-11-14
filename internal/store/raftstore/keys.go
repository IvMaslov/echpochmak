package raftstore

import (
	"encoding/json"
	"fmt"

	"echpochmak/internal/store"
)

// DefaultPrefix is the key prefix of the catalog's state in the KV store.
// The version leaves room for a later change of the layout.
const DefaultPrefix = "cat/v1/"

// Key spaces under the prefix.
const (
	spaceNamespaces = "ns/"
	spaceTables     = "tbl/"
	spaceViews      = "view/"
	pingKey         = "ping"
)

// nameSep separates the parent namespace from the name in a key. Validated
// names never contain control characters, so keys are unambiguous, and the
// children of one parent are exactly the keys with prefix "<parent>\x00":
// grandchildren have a longer parent and never match.
const nameSep = "\x00"

// keys builds the KV keys of one catalog.
type keys struct {
	prefix string
}

// child returns the key of name inside parent, in the given key space.
func (k keys) child(space string, parent store.Namespace, name string) []byte {
	return []byte(k.prefix + space + parent.Encode() + nameSep + name)
}

// children returns the prefix of every key directly inside parent.
func (k keys) children(space string, parent store.Namespace) []byte {
	return []byte(k.prefix + space + parent.Encode() + nameSep)
}

func (k keys) namespace(ns store.Namespace) []byte {
	return k.child(spaceNamespaces, ns.Parent(), ns.Name())
}

func (k keys) ping() []byte {
	return []byte(k.prefix + pingKey)
}

// nameOf returns the name part of a key built by child.
func nameOf(key, childrenPrefix []byte) string {
	return string(key[len(childrenPrefix):])
}

// prefixEnd returns the smallest key greater than every key with prefix.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++

			return end[:i+1]
		}
	}

	return []byte{0}
}

// namespaceValue is the JSON value of a namespace key.
type namespaceValue struct {
	Properties map[string]string `json:"properties"`
}

// pointerValue is the JSON value of a table or view key.
type pointerValue struct {
	MetadataLocation string `json:"metadata_location"`
}

func encode(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encoding value: %w", err)
	}

	return raw, nil
}

func decode(raw []byte, v any) error {
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("decoding stored value: %w", err)
	}

	return nil
}

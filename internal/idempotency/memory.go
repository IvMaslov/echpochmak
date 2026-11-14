package idempotency

import (
	"context"
	"sync"
	"time"
)

// MemoryStore keeps records in memory, for a single catalog replica and for
// tests.
type MemoryStore struct {
	mu      sync.Mutex
	version int64
	records map[string]Record
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: map[string]Record{}}
}

// Create implements [Store].
func (m *MemoryStore) Create(_ context.Context, key string, rec Record) (Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.records[key]; ok {
		return existing, false, nil
	}

	return m.put(key, rec), true, nil
}

// Replace implements [Store].
func (m *MemoryStore) Replace(_ context.Context, key string, version int64, rec Record) (Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.records[key]; !ok || existing.Version != version {
		return Record{}, false, nil
	}

	return m.put(key, rec), true, nil
}

// Delete implements [Store].
func (m *MemoryStore) Delete(_ context.Context, key string, version int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.records[key]; !ok || existing.Version != version {
		return false, nil
	}

	delete(m.records, key)

	return true, nil
}

// Expire implements [Store].
func (m *MemoryStore) Expire(_ context.Context, now time.Time, limit int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := 0

	for key := range m.records {
		if n == limit {
			break
		}

		if !now.Before(m.records[key].ExpiresAt) {
			delete(m.records, key)
			n++
		}
	}

	return n, nil
}

func (m *MemoryStore) put(key string, rec Record) Record {
	m.version++
	rec.Version = m.version
	m.records[key] = rec

	return rec
}

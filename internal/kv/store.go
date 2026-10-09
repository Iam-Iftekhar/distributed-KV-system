package kv

import (
	"sync"
)

// Store defines the state machine interface for the KV store.
type Store interface {
	Set(key, value string)
	Get(key string) (string, bool)
	Delete(key string)
}

// MemoryStore is an in-memory, thread-safe implementation of Store.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemoryStore initializes a new MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string]string),
	}
}

// Set adds or updates a key-value pair.
func (s *MemoryStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves a value by key. Returns the value and a boolean indicating if it exists.
func (s *MemoryStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

// Delete removes a key-value pair.
func (s *MemoryStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
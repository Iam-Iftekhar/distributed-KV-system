package kv

import (
	"strconv"
	"sync"
	"testing"
)

func TestMemoryStore_BasicOperations(t *testing.T) {
	store := NewMemoryStore()

	_, ok := store.Get("unknown")
	if ok {
		t.Error("Expected false for non-existent key")
	}

	store.Set("name", "Benimaru")
	val, ok := store.Get("name")
	if !ok || val != "Benimaru" {
		t.Errorf("Expected 'Benimaru', got '%s'", val)
	}

	store.Delete("name")
	_, ok = store.Get("name")
	if ok {
		t.Error("Expected false after deletion")
	}
}

func TestMemoryStore_Concurrency(t *testing.T) {
	store := NewMemoryStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "key" + strconv.Itoa(n)
			store.Set(key, "value")
		}(i)
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "key" + strconv.Itoa(n)
			store.Get(key)
		}(i)
	}

	wg.Wait()
}
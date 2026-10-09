package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	entries := []LogEntry{
		{Op: "SET", Key: "k1", Value: "v1"},
		{Op: "SET", Key: "k2", Value: "v2"},
		{Op: "DELETE", Key: "k1"},
	}

	for _, e := range entries {
		if err := wal.Write(e); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	wal.Close()

	// Re-open WAL and verify replay
	wal, err = OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal.Close()

	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if len(replayed) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(replayed))
	}

	for i, e := range replayed {
		if e.Op != entries[i].Op || e.Key != entries[i].Key || e.Value != entries[i].Value {
			t.Errorf("Mismatch at %d: got %+v, expected %+v", i, e, entries[i])
		}
	}
}

func TestWAL_CorruptedTail(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "corrupted.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	wal.Write(LogEntry{Op: "SET", Key: "k1", Value: "v1"})
	wal.Close()

	// Corrupt trailing byte intentionally
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("Failed to open wal for corruption: %v", err)
	}
	f.Write([]byte{0xFF, 0xFF}) // Truncated header framing
	f.Close()

	wal, err = OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen corrupt WAL: %v", err)
	}
	defer wal.Close()

	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay failed on corrupted tail: %v", err)
	}

	if len(replayed) != 1 {
		t.Fatalf("Expected 1 clean entry replayed, got %d", len(replayed))
	}
}
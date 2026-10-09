package raft

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRaft_SnapshotCompaction(t *testing.T) {
	dir := t.TempDir()
	p1 := NewPersister(filepath.Join(dir, "r1.json"), filepath.Join(dir, "s1.snap"))

	net := NewMemoryNetwork()
	peers := []string{"node1"}
	ch1 := make(chan ApplyMsg, 100)

	node1 := NewRaft("node1", peers, net, p1, ch1)
	net.Register("node1", node1)
	defer node1.Stop()

	time.Sleep(200 * time.Millisecond)

	// Submit 5 commands
	for i := 1; i <= 5; i++ {
		node1.Start("cmd")
	}

	time.Sleep(100 * time.Millisecond)

	// Truncate log up to index 3 with dummy snapshot
	node1.Snapshot(3, []byte("snapshot_data_at_3"))

	node1.mu.Lock()
	logLen := len(node1.log)
	lastInc := node1.lastIncludedIndex
	node1.mu.Unlock()

	// Remaining log: dummy index 3 + index 4 + index 5 = 3 items
	if logLen != 3 {
		t.Errorf("Expected log length 3 after snapshot, got %d", logLen)
	}
	if lastInc != 3 {
		t.Errorf("Expected lastIncludedIndex 3, got %d", lastInc)
	}
}
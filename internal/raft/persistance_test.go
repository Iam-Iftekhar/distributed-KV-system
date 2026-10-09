package raft

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRaft_PersistenceRecovery(t *testing.T) {
	dir := t.TempDir()
	net := NewMemoryNetwork()
	peers := []string{"node1", "node2", "node3"}

	p1 := NewPersister(filepath.Join(dir, "node1.json"))
	p2 := NewPersister(filepath.Join(dir, "node2.json"))
	p3 := NewPersister(filepath.Join(dir, "node3.json"))

	ch1 := make(chan ApplyMsg, 100)
	ch2 := make(chan ApplyMsg, 100)
	ch3 := make(chan ApplyMsg, 100)

	node1 := NewRaft("node1", peers, net, p1, ch1)
	node2 := NewRaft("node2", peers, net, p2, ch2)
	node3 := NewRaft("node3", peers, net, p3, ch3)

	net.Register("node1", node1)
	net.Register("node2", node2)
	net.Register("node3", node3)

	time.Sleep(350 * time.Millisecond)

	// Identify Leader and write entry
	var leader *Raft
	for _, n := range []*Raft{node1, node2, node3} {
		if _, isLeader := n.GetState(); isLeader {
			leader = n
			break
		}
	}

	if leader == nil {
		t.Fatal("Leader not elected")
	}

	idx, term, _ := leader.Start("SET state active")
	if idx != 1 {
		t.Fatalf("Expected log index 1, got %d", idx)
	}

	time.Sleep(150 * time.Millisecond)

	// Simulate crash of Node 1 by stopping it
	node1.Stop()

	// Restart Node 1 using the same persister file
	ch1New := make(chan ApplyMsg, 100)
	node1Reboot := NewRaft("node1", peers, net, p1, ch1New)
	net.Register("node1", node1Reboot)
	defer node1Reboot.Stop()
	defer node2.Stop()
	defer node3.Stop()

	time.Sleep(200 * time.Millisecond)

	// Verify Node 1 restored term and log entry from disk
	node1Reboot.mu.Lock()
	restoredTerm := node1Reboot.currentTerm
	logLen := len(node1Reboot.log)
	restoredCmd := node1Reboot.log[1].Command
	node1Reboot.mu.Unlock()

	if restoredTerm < term {
		t.Errorf("Expected restored term >= %d, got %d", term, restoredTerm)
	}
	if logLen != 2 { // Index 0 (dummy) + Index 1 (command)
		t.Errorf("Expected log length 2, got %d", logLen)
	}
	if restoredCmd != "SET state active" {
		t.Errorf("Expected 'SET state active', got %v", restoredCmd)
	}
}
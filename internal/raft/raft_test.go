package raft

import (
	"testing"
	"time"
)

func TestRaft_InitialElection(t *testing.T) {
	net := NewMemoryNetwork()
	peers := []string{"node1", "node2", "node3"}

	applyCh1 := make(chan ApplyMsg, 100)
	applyCh2 := make(chan ApplyMsg, 100)
	applyCh3 := make(chan ApplyMsg, 100)

	// Pass `nil` as the 4th argument (persister)
	node1 := NewRaft("node1", peers, net, nil, applyCh1)
	node2 := NewRaft("node2", peers, net, nil, applyCh2)
	node3 := NewRaft("node3", peers, net, nil, applyCh3)

	net.Register("node1", node1)
	net.Register("node2", node2)
	net.Register("node3", node3)

	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()

	time.Sleep(350 * time.Millisecond)

	leaders := 0
	for _, n := range []*Raft{node1, node2, node3} {
		_, isLeader := n.GetState()
		if isLeader {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf("Expected exactly 1 leader, got %d", leaders)
	}
}

func TestRaft_Replication(t *testing.T) {
	net := NewMemoryNetwork()
	peers := []string{"node1", "node2", "node3"}

	chans := map[string]chan ApplyMsg{
		"node1": make(chan ApplyMsg, 100),
		"node2": make(chan ApplyMsg, 100),
		"node3": make(chan ApplyMsg, 100),
	}

	nodes := make(map[string]*Raft)
	for _, id := range peers {
		n := NewRaft(id, peers, net, nil, chans[id])
		nodes[id] = n
		net.Register(id, n)
		defer n.Stop()
	}

	time.Sleep(350 * time.Millisecond)

	var leader *Raft
	for _, n := range nodes {
		if _, isLeader := n.GetState(); isLeader {
			leader = n
			break
		}
	}

	if leader == nil {
		t.Fatal("No leader found")
	}

	idx, _, isLeader := leader.Start("SET user benimaru")
	if !isLeader || idx != 1 {
		t.Fatalf("Failed to start command on leader: idx=%d, isLeader=%v", idx, isLeader)
	}

	for _, id := range peers {
		select {
		case msg := <-chans[id]:
			if msg.CommandIndex != 1 || msg.Command != "SET user benimaru" {
				t.Fatalf("[%s] Unexpected apply msg: %+v", id, msg)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("[%s] Timed out waiting for log commit", id)
		}
	}
}
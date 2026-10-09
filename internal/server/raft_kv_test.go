package server

import (
	"context"
	"testing"
	"time"

	"github.com/iam-iftekhar/kvraft/internal/kv"
	"github.com/iam-iftekhar/kvraft/internal/raft"
)

func TestRaftKV_ReplicatedState(t *testing.T) {
	net := raft.NewMemoryNetwork()
	peers := []string{"node1", "node2", "node3"}

	servers := make(map[string]*RaftKV)
	raftNodes := make(map[string]*raft.Raft)

	for _, id := range peers {
		applyCh := make(chan raft.ApplyMsg, 100)
		rf := raft.NewRaft(id, peers, net, nil, applyCh)
		store := kv.NewMemoryStore()

		kvServer := NewRaftKV(id, rf, store, applyCh)

		net.Register(id, rf)
		raftNodes[id] = rf
		servers[id] = kvServer

		defer rf.Stop()
		defer kvServer.Stop()
	}

	// Wait for election
	time.Sleep(350 * time.Millisecond)

	// Find leader server
	var leaderID string
	var leaderKV *RaftKV
	for id, rf := range raftNodes {
		if _, isLeader := rf.GetState(); isLeader {
			leaderID = id
			leaderKV = servers[id]
			break
		}
	}

	if leaderKV == nil {
		t.Fatal("Leader election failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Write SET key via Leader
	setRes := leaderKV.Execute(ctx, OpCommand{
		Op:       "SET",
		Key:      "cluster_status",
		Value:    "healthy",
		ClientID: "client1",
		SeqNum:   1,
	})

	if setRes.Err != nil {
		t.Fatalf("SET operation failed: %v", setRes.Err)
	}

	// Wait for replication across all nodes
	time.Sleep(150 * time.Millisecond)

	// 2. Verify all nodes (including non-leaders) have key in state machine
	for _, id := range peers {
		val, found := servers[id].kvStore.Get("cluster_status")
		if !found || val != "healthy" {
			t.Errorf("[%s] Expected 'healthy', got val='%s', found=%v", id, val, found)
		}
	}

	// 3. Perform GET via Leader
	getRes := leaderKV.Execute(ctx, OpCommand{
		Op:  "GET",
		Key: "cluster_status",
	})

	if getRes.Err != nil || !getRes.Found || getRes.Value != "healthy" {
		t.Fatalf("GET operation failed: %+v", getRes)
	}

	t.Logf("Replication test passed. Executed via Leader %s", leaderID)
}
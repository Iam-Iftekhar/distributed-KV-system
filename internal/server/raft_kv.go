package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/iam-iftekhar/kvraft/internal/kv"
	"github.com/iam-iftekhar/kvraft/internal/raft"
)

var (
	ErrWrongLeader = errors.New("raft_kv: node is not the leader")
	ErrTimeout     = errors.New("raft_kv: operation timed out waiting for consensus")
)

type OpCommand struct {
	Op       string `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value"`
	ClientID string `json:"client_id"`
	SeqNum   int64  `json:"seq_num"`
}

type OpResult struct {
	Value string
	Found bool
	Err   error
}

type RaftKV struct {
	mu      sync.Mutex
	me      string
	rf      *raft.Raft
	kvStore kv.Store

	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}

	// Pending client requests waiting for log indices to commit: index -> result channel
	waitChs map[int]chan OpResult

	// Client request deduplication state: clientID -> last executed seqNum & result
	lastAppliedSeq map[string]int64
}

func NewRaftKV(me string, rf *raft.Raft, kvStore kv.Store, applyCh chan raft.ApplyMsg) *RaftKV {
	kvServer := &RaftKV{
		me:             me,
		rf:             rf,
		kvStore:        kvStore,
		applyCh:        applyCh,
		stopCh:         make(chan struct{}),
		waitChs:        make(map[int]chan OpResult),
		lastAppliedSeq: make(map[string]int64),
	}

	go kvServer.applierLoop()

	return kvServer
}

func (kv *RaftKV) Stop() {
	close(kv.stopCh)
}

// Execute submits a key-value operation to Raft and waits for commitment.
func (kv *RaftKV) Execute(ctx context.Context, cmd OpCommand) OpResult {
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return OpResult{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.waitChs[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitChs, index)
		kv.mu.Unlock()
	}()

	select {
	case result := <-ch:
		return result
	case <-ctx.Done():
		return OpResult{Err: ErrTimeout}
	case <-time.After(2 * time.Second):
		return OpResult{Err: ErrTimeout}
	}
}

// applierLoop processes committed entries from Raft's applyCh.
func (kv *RaftKV) applierLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg, ok := <-kv.applyCh:
			if !ok {
				return
			}

			if !msg.CommandValid {
				continue
			}

			cmd, ok := msg.Command.(OpCommand)
			if !ok {
				continue
			}

			kv.mu.Lock()
			result := kv.applyToStore(cmd)

			// Notify waiting RPC handler if this node submitted the log entry
			if ch, exists := kv.waitChs[msg.CommandIndex]; exists {
				ch <- result
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *RaftKV) applyToStore(cmd OpCommand) OpResult {
	// Deduplication check for mutating commands
	if cmd.Op != "GET" && cmd.ClientID != "" {
		if lastSeq, exists := kv.lastAppliedSeq[cmd.ClientID]; exists && cmd.SeqNum <= lastSeq {
			// Duplicate operation, return cached result without mutating store
			val, found := kv.kvStore.Get(cmd.Key)
			return OpResult{Value: val, Found: found}
		}
		kv.lastAppliedSeq[cmd.ClientID] = cmd.SeqNum
	}

	res := OpResult{}
	switch cmd.Op {
	case "SET":
		kv.kvStore.Set(cmd.Key, cmd.Value)
		res.Value = cmd.Value
		res.Found = true
	case "GET":
		res.Value, res.Found = kv.kvStore.Get(cmd.Key)
	case "DELETE":
		res.Value, res.Found = kv.kvStore.Get(cmd.Key)
		kv.kvStore.Delete(cmd.Key)
	}

	return res
}
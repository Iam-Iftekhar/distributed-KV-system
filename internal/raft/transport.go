package raft

import (
	"context"
	"errors"
	"sync"
)

var ErrNodeNotFound = errors.New("raft: node not found in network")

// Transport abstracts network calls between Raft nodes.
type Transport interface {
	SendRequestVote(ctx context.Context, target string, args *RequestVoteArgs, reply *RequestVoteReply) error
	SendAppendEntries(ctx context.Context, target string, args *AppendEntriesArgs, reply *AppendEntriesReply) error
}

// MemoryNetwork simulates an in-memory network for deterministic Raft testing.
type MemoryNetwork struct {
	mu    sync.RWMutex
	nodes map[string]*Raft
}

func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{
		nodes: make(map[string]*Raft),
	}
}

func (m *MemoryNetwork) Register(id string, node *Raft) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[id] = node
}

func (m *MemoryNetwork) SendRequestVote(ctx context.Context, target string, args *RequestVoteArgs, reply *RequestVoteReply) error {
	m.mu.RLock()
	node, ok := m.nodes[target]
	m.mu.RUnlock()

	if !ok {
		return ErrNodeNotFound
	}

	*reply = node.RequestVote(args)
	return nil
}

func (m *MemoryNetwork) SendAppendEntries(ctx context.Context, target string, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	m.mu.RLock()
	node, ok := m.nodes[target]
	m.mu.RUnlock()

	if !ok {
		return ErrNodeNotFound
	}

	*reply = node.AppendEntries(args)
	return nil
}
package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type PersistentState struct {
	CurrentTerm int        `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

type Persister struct {
	mu        sync.Mutex
	raftPath  string
	snapPath  string
}

func NewPersister(raftPath, snapPath string) *Persister {
	return &Persister{
		raftPath: raftPath,
		snapPath: snapPath,
	}
}

func (p *Persister) Save(state PersistentState, snapshot []byte) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	// 1. Save Raft State
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := atomicWrite(p.raftPath, data); err != nil {
		return err
	}

	// 2. Save Snapshot Data if provided
	if len(snapshot) > 0 {
		if err := atomicWrite(p.snapPath, snapshot); err != nil {
			return err
		}
	}

	return nil
}

func (p *Persister) ReadRaftState() (*PersistentState, error) {
	if p == nil {
		return nil, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.raftPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (p *Persister) ReadSnapshot() ([]byte, error) {
	if p == nil {
		return nil, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.snapPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(dir, "raft-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return err
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return err
	}
	tmpFile.Close()

	return os.Rename(tmpName, path)
}
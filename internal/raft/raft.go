package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type NodeRole string

const (
	Follower  NodeRole = "FOLLOWER"
	Candidate NodeRole = "CANDIDATE"
	Leader    NodeRole = "LEADER"
)

type LogEntry struct {
	Term    int         `json:"term"`
	Index   int         `json:"index"`
	Command interface{} `json:"command"`
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.Mutex
	peers     []string
	me        string
	transport Transport
	persister *Persister

	// Persistent state on all servers
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  map[string]int
	matchIndex map[string]int

	// Role & Timers
	role            NodeRole
	lastResetTime   time.Time
	electionTimeout time.Duration

	applyCh chan ApplyMsg
	stopCh  chan struct{}
}

func NewRaft(me string, peers []string, transport Transport, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		me:              me,
		peers:           peers,
		transport:       transport,
		persister:       persister,
		role:            Follower,
		currentTerm:     0,
		votedFor:        "",
		log:             make([]LogEntry, 0),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make(map[string]int),
		matchIndex:      make(map[string]int),
		applyCh:         applyCh,
		stopCh:          make(chan struct{}),
		lastResetTime:   time.Now(),
		electionTimeout: getRandomTimeout(),
	}

	// 1-based log indexing (index 0 is a dummy entry)
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})

	// Restore state if persisted data exists on disk
	rf.readPersist()

	go rf.tickerLoop()

	return rf
}

func (rf *Raft) Stop() {
	close(rf.stopCh)
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func getRandomTimeout() time.Duration {
	// 150ms to 300ms randomized timeout
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) resetElectionTimeout() {
	rf.lastResetTime = time.Now()
	rf.electionTimeout = getRandomTimeout()
}

func (rf *Raft) persist() {
	if rf.persister == nil {
		return
	}
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	_ = rf.persister.Save(state)
}

func (rf *Raft) readPersist() {
	if rf.persister == nil {
		return
	}
	state, err := rf.persister.Read()
	if err != nil || state == nil {
		return
	}

	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	if len(state.Log) > 0 {
		rf.log = state.Log
	}
}

func (rf *Raft) tickerLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
			rf.mu.Lock()
			switch rf.role {
			case Follower, Candidate:
				if time.Since(rf.lastResetTime) >= rf.electionTimeout {
					rf.startElection()
				}
			case Leader:
				rf.broadcastHeartbeats()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimeout()

	term := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	votesGranted := 1 // Vote for self

	for _, peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peerID string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := rf.transport.SendRequestVote(ctx, peerID, &args, &reply)
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != Candidate || rf.currentTerm != term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				votesGranted++
				if votesGranted > (len(rf.peers) / 2) {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = ""
	rf.persist()
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	for _, peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
	rf.broadcastHeartbeats()
}

// Start appends a command to the Leader log and initiates replication.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	rf.persist()
	rf.broadcastHeartbeats()

	return index, term, true
}

// Add fields to Raft struct:
// lastIncludedIndex int
// lastIncludedTerm  int

// Snapshot truncates the Raft log up to index and persists snapshot bytes.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}

	// Calculate slice position offset by lastIncludedIndex
	relIndex := index - rf.lastIncludedIndex
	rf.lastIncludedTerm = rf.log[relIndex].Term
	rf.lastIncludedIndex = index

	// Retain remaining uncompacted log entries (index 0 becomes dummy with lastIncludedTerm)
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Term: rf.lastIncludedTerm, Index: index})
	newLog = append(newLog, rf.log[relIndex+1:]...)
	rf.log = newLog

	rf.persistWithSnapshot(snapshot)
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	if rf.persister == nil {
		return
	}
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	_ = rf.persister.Save(state, snapshot)
}
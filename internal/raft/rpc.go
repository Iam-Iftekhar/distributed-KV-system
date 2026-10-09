package raft

import (
	"context"
	"time"
)

type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

func (rf *Raft) RequestVote(args *RequestVoteArgs) RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := RequestVoteReply{
		Term:        rf.currentTerm,
		VoteGranted: false,
	}

	if args.Term < rf.currentTerm {
		return reply
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	// Raft Election Restriction (Section 5.4.1):
	// Candidate's log must be at least as up-to-date as receiver's log
	upToDate := false
	if args.LastLogTerm > lastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		upToDate = true
	}

	if (rf.votedFor == "" || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimeout()
		reply.VoteGranted = true
	}

	reply.Term = rf.currentTerm
	return reply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs) AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: false,
	}

	if args.Term < rf.currentTerm {
		return reply
	}

	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.becomeFollower(args.Term)
	}

	rf.resetElectionTimeout()

	// Log Consistency Check (Section 5.3)
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return reply
	}

	// Truncate conflicting entries and append new ones
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()

	// Update Commit Index
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := len(rf.log) - 1
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
		rf.applyEntries()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	return reply
}

func (rf *Raft) broadcastHeartbeats() {
	term := rf.currentTerm

	for _, peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[peer] - 1
		if prevLogIndex >= len(rf.log) {
			prevLogIndex = len(rf.log) - 1
		}
		prevLogTerm := rf.log[prevLogIndex].Term

		entries := make([]LogEntry, len(rf.log[prevLogIndex+1:]))
		copy(entries, rf.log[prevLogIndex+1:])

		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func(peerID string, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := rf.transport.SendAppendEntries(ctx, peerID, &args, &reply)
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != Leader || rf.currentTerm != args.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				rf.nextIndex[peerID] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peerID] = args.PrevLogIndex + len(args.Entries)
				rf.checkAndAdvanceCommitIndex()
			} else if rf.nextIndex[peerID] > 1 {
				// Decrement nextIndex on rejection to find log match point
				rf.nextIndex[peerID]--
			}
		}(peer, args)
	}
}

func (rf *Raft) checkAndAdvanceCommitIndex() {
	// Count matchIndex across peers to find majority commit index
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		// Leader can only commit log entries from its current term directly (Raft Section 5.4.2)
		if rf.log[N].Term != rf.currentTerm {
			continue
		}

		count := 1 // Leader itself
		for _, peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyEntries()
			break
		}
	}
}

func (rf *Raft) applyEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
}

type InstallSnapshotArgs struct {
	Term              int    `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex int    `json:"last_included_index"`
	LastIncludedTerm  int    `json:"last_included_term"`
	Data              []byte `json:"data"`
}

type InstallSnapshotReply struct {
	Term int `json:"term"`
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs) InstallSnapshotReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := InstallSnapshotReply{Term: rf.currentTerm}

	if args.Term < rf.currentTerm {
		return reply
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.resetElectionTimeout()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return reply
	}

	// Truncate log if matching entry found, else clear log
	relIndex := args.LastIncludedIndex - rf.lastIncludedIndex
	if relIndex < len(rf.log) && rf.log[relIndex].Term == args.LastIncludedTerm {
		newLog := make([]LogEntry, 0)
		newLog = append(newLog, rf.log[relIndex:]...)
		rf.log = newLog
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persistWithSnapshot(args.Data)

	// Send snapshot to application state machine via applyCh
	go func(snapData []byte) {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapData,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}(args.Data)

	return reply
}
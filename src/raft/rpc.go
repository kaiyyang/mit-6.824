package raft

import (
	"fmt"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (reply RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term:%v,CandidateId:%v,LastLogIndex:%v,LastLogTerm:%v}", reply.Term, reply.CandidateId, reply.LastLogIndex, reply.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("{Term:%v,VoteGranted:%v}", reply.Term, reply.VoteGranted)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,PrevLogTerm:%v,LeaderCommit:%v,Entries:%v}", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XIndex  int // index of first entry with that term (if any)
	XTerm   int // term in the conflicting entry (if any)
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v}", reply.Term, reply.Success)
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func (request InstallSnapshotRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,LastIncludedIndex:%v,LastIncludedTerm:%v,DataSize:%v}", request.Term, request.LeaderId, request.LastIncludedIndex, request.LastIncludedTerm, len(request.Data))
}

type InstallSnapshotResponse struct {
	Term int
}

func (response InstallSnapshotResponse) String() string {
	return fmt.Sprintf("{Term:%v}", response.Term)
}

package raft

type RequestVoteArgs struct {
	Me int
	// for vote
	Term               int
	LeaderLastLogIndex int
	LeaderLastLogTerm  int
}

type RequestVoteReply struct {
	Term    int
	VoteFor bool // whether voted
}

type AppendEntryArgs struct {
	Me                  int
	LeaderLastCommitted int

	// for leadership confirm
	Term               int
	LeaderLastLogIndex int
	LeaderLastLogTerm  int

	// for log replication
	LastLogTerm  int
	LastLogIndex int
	Logs         *LogEntry
	Snapshot     *Snapshot
}

type AppendEntryReply struct {
	Term              int
	ExpectedNextIndex int
	Success           bool
}

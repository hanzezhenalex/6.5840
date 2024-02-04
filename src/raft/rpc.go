package raft

type RequestVoteArgs struct {
	Term int
	Me   int
}

type RequestVoteReply struct {
	Term    int
	VoteFor bool
}

type AppendEntryArgs struct {
	Term          int
	Me            int
	LastCommitted int
	LastLogIndex  int
	LastLogTerm   int

	Logs []LogEntry
}

type AppendEntryReply struct {
	Term              int
	ExpectedNextIndex int
}

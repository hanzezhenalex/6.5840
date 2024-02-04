package raft

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type LogManager struct {
	lastCommitted int

	lastLogIndex int
	lastLogTerm  int
	logs         []LogEntry
}

func NewLogManager() *LogManager {
	return &LogManager{
		lastCommitted: -1,
		lastLogIndex:  -1,
		lastLogTerm:   -1,
	}
}

func (lm *LogManager) GetLastCommittedLogIndex() int {
	return lm.lastCommitted
}

func (lm *LogManager) GetLastLogIndex() int {
	return lm.lastLogIndex
}

func (lm *LogManager) GetLastLogTerm() int {
	return lm.lastLogTerm
}

func (lm *LogManager) Append(logs []LogEntry) {
	if len(logs) == 0 {
		return
	}

	lm.logs = append(lm.logs, logs...)
	lm.lastLogIndex = logs[len(logs)-1].Index
	lm.lastLogTerm = logs[len(logs)-1].Term
}

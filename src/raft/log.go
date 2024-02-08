package raft

import "go.uber.org/zap"

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type LogManager struct {
	lastLogIndex int
	lastLogTerm  int
	logs         []*LogEntry

	logger *zap.Logger
}

func NewLogManager() *LogManager {
	return &LogManager{
		lastLogIndex: -1,
		lastLogTerm:  -1,
		logs:         make([]*LogEntry, 0, 10),
		logger:       GetLoggerOrPanic("log mngr"),
	}
}

func (lm *LogManager) New() *LogManager {
	newLm := new(LogManager)
	*newLm = *lm
	return newLm
}

func (lm *LogManager) GetLastLogIndex() int {
	return lm.lastLogIndex
}

func (lm *LogManager) GetLastLogTerm() int {
	return lm.lastLogTerm
}

func (lm *LogManager) GetLogByIndex(index int) (*LogEntry, error) {
	if index < 0 {
		panic(errorIllegalLogIndex)
	}
	if lm.GetLastLogIndex() < index {
		return nil, errorLogIndexOutOfRange
	}
	return lm.logs[index], nil
}

// Truncate truncates the logs from n, keep [0:n)
func (lm *LogManager) Truncate(index int) {
	if index > lm.lastLogIndex+1 {
		panic("truncate: index bigger than lastLogIndex+1")
	}
	if index < 0 {
		panic("truncate: index smaller than zero")
	}
	lm.logs = lm.logs[:index]
	if index == 0 {
		lm.lastLogTerm = -1
		lm.lastLogIndex = -1
	} else {
		lm.lastLogTerm = lm.logs[len(lm.logs)-1].Term
		lm.lastLogIndex = lm.logs[len(lm.logs)-1].Index
	}
}

func (lm *LogManager) AppendLogAndReturnNextIndex(lastLogIndex, lastLogTerm int, log *LogEntry) (int, bool) {
	if log == nil { // heartbeat
		lm.logger.Debug("receive heartbeat")
		return lm.GetLastLogIndex() + 1, false
	}

	match := func(index int, term int) bool {
		if index == -1 {
			return true
		}
		mylog, err := lm.GetLogByIndex(index)
		if err == errorLogIndexOutOfRange {
			return false
		}
		return mylog.Term == term
	}

	if match(log.Index, log.Term) {
		lm.logger.Debug("log matched, want next")
		return log.Index + 1, false
	} else if match(lastLogIndex, lastLogTerm) {
		lm.logger.Debug("last log matched, append")
		lm.Truncate(lastLogIndex + 1)
		lm.appendOneLog(log)
		return log.Index + 1, true
	} else {
		lm.logger.Debug("no log matched, try older")
		return lastLogIndex, false
	}
}

func (lm *LogManager) AppendNewLog(term int, command interface{}) {
	lm.appendOneLog(&LogEntry{
		Command: command,
		Term:    term,
		Index:   lm.lastLogIndex + 1,
	})
}

func (lm *LogManager) appendOneLog(log *LogEntry) {
	if log.Index != lm.lastLogIndex+1 {
		panic("new log index should equal to lastLogIndex+1")
	}

	lm.logs = append(lm.logs, log)
	lm.lastLogIndex = log.Index
	lm.lastLogTerm = log.Term
}

package raft

import "go.uber.org/zap"

const (
	IndexStartFrom = 1
	EmptyLogIndex  = 0
)

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
	lm := &LogManager{
		logs:   make([]*LogEntry, 0, 10),
		logger: GetLoggerOrPanic("log mngr"),
	}
	lm.reset()
	return lm
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

func (lm *LogManager) logIndexToInternalIndex(logIndex int) int {
	return logIndex - 1
}

func (lm *LogManager) GetLogByIndex(logIndex int) (*LogEntry, error) {
	if logIndex < IndexStartFrom {
		panic(errorIllegalLogIndex)
	}
	if lm.GetLastLogIndex() < logIndex {
		return nil, errorLogIndexOutOfRange
	}
	return lm.logs[lm.logIndexToInternalIndex(logIndex)], nil
}

// Truncate truncates the logs from n, keep [0:n)
func (lm *LogManager) Truncate(logIndex int) {
	if logIndex > lm.lastLogIndex+1 {
		panic("truncate: index bigger than lastLogIndex+1")
	}
	if logIndex < IndexStartFrom {
		panic("truncate: index smaller than start index")
	}

	internalIndex := lm.logIndexToInternalIndex(logIndex)

	lm.logs = lm.logs[:internalIndex]
	if internalIndex == 0 {
		lm.reset()
	} else {
		lastLog := lm.logs[len(lm.logs)-1]
		lm.updateLastLog(lastLog.Term, lastLog.Index)
	}
}

func (lm *LogManager) AppendLogAndReturnNextIndex(lastLogIndex, lastLogTerm int, log *LogEntry) (int, bool) {
	if log == nil { // heartbeat
		lm.logger.Debug("receive heartbeat")
		return lm.GetLastLogIndex() + 1, false
	}

	match := func(logIndex int, term int) bool {
		if logIndex < IndexStartFrom {
			return true
		}
		mylog, err := lm.GetLogByIndex(logIndex)
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

func (lm *LogManager) reset() {
	lm.lastLogTerm = -1
	lm.lastLogIndex = EmptyLogIndex
}

func (lm *LogManager) updateLastLog(term, index int) {
	lm.lastLogTerm = term
	lm.lastLogIndex = index
}

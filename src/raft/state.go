package raft

type StateManager struct {
	committed int
	term      int
	logMngr   *LogManager
}

func (sm *StateManager) New() *StateManager {
	return &StateManager{
		committed: sm.committed,
		term:      sm.term,
		logMngr:   sm.logMngr.New(),
	}
}

func (sm *StateManager) IsLogAheadPeer(peerLastLogIndex int, peerLastLogTerm int) bool {
	if sm.logMngr.GetLastLogTerm() > peerLastLogTerm {
		return true
	} else if sm.logMngr.GetLastLogTerm() == peerLastLogTerm {
		return sm.logMngr.GetLastLogIndex() > peerLastLogIndex
	}
	return false
}

func (sm *StateManager) GetCurrentTerm() int {
	return sm.term
}

func (sm *StateManager) IncrTerm() {
	sm.term++
}

func (sm *StateManager) UpdateTerm(newTerm int) {
	if newTerm <= sm.term {
		panic("new term should not be equal/smaller than current one")
	}
	sm.term = newTerm
}

func (sm *StateManager) UpdateCommitted(committed int) bool {
	if sm.committed < committed {
		sm.committed = committed
		return true
	}
	return false
}

func (sm *StateManager) GetCommitted() int {
	return sm.committed
}

func (sm *StateManager) StateBehindPeer(term int) bool {
	if sm.term >= term {
		return false
	}
	sm.term = term
	return true
}

func (sm *StateManager) SyncStateFromAppendEntriesTask(task *AppendEntriesTask) {
	currentTerm := sm.GetCurrentTerm()
	peerTerm := task.args.Term

	if currentTerm < peerTerm {
		sm.UpdateTerm(peerTerm)
	}

	nextIndex, logAppended := sm.logMngr.AppendLogAndReturnNextIndex(
		task.args.LastLogIndex,
		task.args.LastLogTerm,
		task.args.Logs,
	)
	// peer needs these for committing and deciding next log to send
	task.reply.ExpectedNextIndex = nextIndex
	task.reply.Success = logAppended
	task.reply.Term = sm.GetCurrentTerm()

	if task.reply.Success || task.args.Logs == nil {
		// Success == false means log is matching
		// update committed in matching phase could commit wrong logs
		sm.UpdateCommitted(task.args.LeaderLastCommitted)
	}
}

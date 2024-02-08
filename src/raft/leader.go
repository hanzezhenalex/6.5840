package raft

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"6.5840/labrpc"

	"go.uber.org/zap"
)

type Leader struct {
	worker *Raft
	logger *zap.Logger
	term   int

	stopCh chan struct{}
	peers  []*replicator
	// shared StateManager for replicators
	replicatorStateMngr *atomic.Value
}

func NewLeader(worker *Raft) *Leader {
	leader := &Leader{
		worker: worker,
		logger: GetLoggerOrPanic("leader").
			With(zap.Int(Term, worker.state.GetCurrentTerm())).
			With(zap.Int(Index, worker.me)),
		stopCh:              make(chan struct{}),
		term:                worker.state.GetCurrentTerm(),
		replicatorStateMngr: new(atomic.Value),
	}

	leader.replicatorStateMngr.Store(worker.state.New())

	for index, p := range worker.peers {
		if index == worker.me {
			continue
		}
		leader.peers = append(
			leader.peers,
			NewReplicator(index, p, worker, leader.stopCh, leader.replicatorStateMngr),
		)
	}
	return leader
}

func (l *Leader) Type() RoleType {
	return RoleLeader
}

func (l *Leader) StartDaemon() {
	for _, peer := range l.peers {
		go peer.daemon()
	}
}

func (l *Leader) StopDaemon() {
	close(l.stopCh)
}

func (l *Leader) HandleNotify() {}

func (l *Leader) HandleRequestVotesTask(task *RequestVotesTask) {
	currentTerm := l.term
	peerTerm := task.args.Term
	logger := l.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		logger.Debug("RequestVote reject, term ahead")
		task.reply.VoteFor = false
	} else if currentTerm == peerTerm {
		l.logger.Debug("RequestVote reject, I'm leader")
		task.reply.VoteFor = false
	} else if l.worker.state.IsLogAheadPeer(
		task.args.LeaderLastLogIndex, task.args.LeaderLastLogTerm,
	) {
		logger.Debug("RequestVote reject, log ahead peer",
			zap.Int("lastLogIndex", l.worker.state.logMngr.GetLastLogIndex()),
			zap.Int("lastLogTerm", l.worker.state.logMngr.GetLastLogTerm()),
			zap.Int("peerLastLogIndex", task.args.LeaderLastLogIndex),
			zap.Int("peerLastLogIndex", task.args.LeaderLastLogTerm),
		)
		task.reply.VoteFor = false
	} else {
		logger.Debug("RequestVote granted")
		l.worker.become(RoleFollower)
		l.worker.state.UpdateTerm(peerTerm)
		task.reply.VoteFor = true
	}
}

func (l *Leader) HandleAppendEntriesTask(task *AppendEntriesTask) {
	currentTerm := l.term
	peerTerm := task.args.Term
	logger := l.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		logger.Debug("AppendEntries reject, term ahead")
	} else if currentTerm == peerTerm {
		panic("two leaders in one term")
	} else {
		logger.Info("found new leader")
		l.worker.become(RoleFollower)
		l.worker.state.SyncStateFromAppendEntriesTask(task)
	}
}

func (l *Leader) UpdateReplicatorState() {
	l.replicatorStateMngr.Store(l.worker.state.New())
}

func (l *Leader) UpdateCommittedIndex() {
	committed := make([]int32, 0, len(l.peers))

	for _, peer := range l.peers {
		committed = append(committed, atomic.LoadInt32(&peer.replicated))
	}
	sort.Slice(committed, func(i, j int) bool {
		return committed[i] < committed[j]
	})

	if tryToCommit := int(committed[len(committed)/2]); tryToCommit >= IndexStartFrom {
		log, err := l.worker.state.logMngr.GetLogByIndex(tryToCommit)
		if err != nil {
			panic(err)
		}

		// only commit logs in this term
		if log.Term == l.term && l.worker.state.UpdateCommitted(tryToCommit) {
			l.UpdateReplicatorState()
		}
	}
}

type replicator struct {
	term      int
	me        int
	peerIndex int
	peer      *labrpc.ClientEnd

	logger     *zap.Logger
	state      *atomic.Value
	nextIndex  int
	replicated int32

	stopCh  chan struct{}
	timeout time.Duration
}

func NewReplicator(
	peerIndex int,
	peer *labrpc.ClientEnd,
	worker *Raft,
	stopCh chan struct{},
	stateMngr *atomic.Value,
) *replicator {
	return &replicator{
		term:      worker.state.GetCurrentTerm(),
		me:        worker.me,
		peerIndex: peerIndex,
		peer:      peer,
		state:     stateMngr,
		logger: GetLoggerOrPanic("replicator").
			With(zap.Int(Peer, peerIndex)).
			With(zap.Int(Term, worker.state.GetCurrentTerm())).
			With(zap.Int(Index, worker.me)),
		stopCh:     stopCh,
		timeout:    worker.heartBeatInterval,
		replicated: -1,
	}
}

func (rp *replicator) initNextIndex() {
	rp.nextIndex = rp.state.Load().(*StateManager).logMngr.GetLastLogIndex()
	if rp.nextIndex == 0 { // no log in the state mngr
		rp.nextIndex = 1
	}
}

func (rp *replicator) daemon() {
	rp.initNextIndex()
	rp.syncLogsWithPeer()
	timer := time.NewTimer(rp.timeout)

	for {
		select {
		case <-rp.stopCh:
			rp.logger.Info("replicator stopped")
			return
		case <-timer.C:
			rp.syncLogsWithPeer()
			timer.Reset(rp.timeout)
		}
	}
}

func (rp *replicator) syncLogsWithPeer() {
	rp.logger.Info("replicator start sync")
	continueSending := true

	for continueSending {
		var (
			args  AppendEntryArgs
			reply AppendEntryReply
		)
		args, continueSending = rp.fillAppendEntriesArgs()

		if stopOnTimeout := rp.withTimeout(func() {
			rp.logger.Debug("call RPC for sync logs")
			if ok := rp.peer.Call("Raft.AppendEntries", args, &reply); !ok {
				rp.logger.Warn("fail to send RPC request to peer")
			}
		}); stopOnTimeout {
			rp.logger.Warn("timeout sending RPC request to peer")
			return
		}

		rp.nextIndex = reply.ExpectedNextIndex
		if reply.Success {
			atomic.StoreInt32(&rp.replicated, int32(args.Logs.Index))
		}
	}
}

func (rp *replicator) fillAppendEntriesArgs() (AppendEntryArgs, bool) {
	stateMngr := rp.state.Load().(*StateManager)
	args := AppendEntryArgs{
		Term:                rp.term,
		Me:                  rp.me,
		LeaderLastCommitted: stateMngr.GetCommitted(),
		LeaderLastLogIndex:  stateMngr.logMngr.GetLastLogIndex(),
		LeaderLastLogTerm:   stateMngr.logMngr.GetLastLogTerm(),
	}

	expectedLog, err := stateMngr.logMngr.GetLogByIndex(rp.nextIndex)
	if err == errorLogIndexOutOfRange {
		rp.logger.Info("all logs replicated to peer, heartbeat only")
		return args, false
	}
	args.Logs = expectedLog

	if previous := rp.nextIndex - 1; previous >= IndexStartFrom {
		previousLog, err := stateMngr.logMngr.GetLogByIndex(previous)
		if err == errorLogIndexOutOfRange {
			panic(fmt.Sprintf("previous log index should not out of range, index=%d", previous))
		}

		args.LastLogIndex = previousLog.Index
		args.LastLogTerm = previousLog.Term
	} else {
		args.LastLogIndex = EmptyLogIndex
		args.LastLogTerm = -1
	}
	return args, true
}

func (rp *replicator) withTimeout(fn func()) bool {
	timer := time.NewTimer(rp.timeout)
	done := make(chan struct{})

	go func() {
		fn()
		select {
		case <-timer.C:
		default:
			done <- struct{}{}
		}
		close(done)
	}()

	select {
	case <-timer.C:
		return true
	case <-done:
		return false
	}
}

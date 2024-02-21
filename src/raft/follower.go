package raft

import (
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	working int32 = 0
	timeout int32 = 1
)

type Follower struct {
	worker *Raft

	timer  *time.Timer
	stopCh chan struct{}
	logger *zap.Logger

	status int32
}

func NewFollower(worker *Raft) *Follower {
	return &Follower{
		worker: worker,
		stopCh: make(chan struct{}),
		timer:  time.NewTimer(worker.Timeout()),
		status: working,
		logger: GetLoggerOrPanic("follower").
			With(zap.Int(Index, worker.me)),
	}
}

func (f *Follower) StartDaemon() {
	f.logger.Info("daemon start working")

LOOP:
	for {
		select {
		case <-f.stopCh:
			f.logger.Debug("daemon stopped")
			break LOOP
		case <-f.timer.C:
			f.logger.Debug("timeout")
			atomic.StoreInt32(&f.status, timeout)
			f.worker.Notify(followerTimeout)
			break LOOP
		}
	}

	f.timer.Stop()
}

func (f *Follower) StopDaemon() {
	close(f.stopCh)
}

func (f *Follower) Type() RoleType { return RoleFollower }

func (f *Follower) HandleRequestVotesTask(task *RequestVotesTask) {
	currentTerm := f.worker.state.GetCurrentTerm()
	peerTerm := task.args.Term
	logger := f.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		logger.Debug("RequestVote reject, term ahead")
		task.reply.VoteFor = false
	} else if currentTerm == peerTerm {
		logger.Debug("RequestVote reject, has voted in this term")
		task.reply.VoteFor = false
	} else if f.worker.state.IsLogAheadPeer(
		task.args.LeaderLastLogIndex, task.args.LeaderLastLogTerm,
	) {
		logger.Debug("RequestVote reject, log ahead peer",
			zap.Int("lastLogIndex", f.worker.state.logMngr.GetLastLogIndex()),
			zap.Int("lastLogTerm", f.worker.state.logMngr.GetLastLogTerm()),
			zap.Int("peerLastLogIndex", task.args.LeaderLastLogIndex),
			zap.Int("peerLastLogIndex", task.args.LeaderLastLogTerm),
		)
		task.reply.VoteFor = false
	} else {
		logger.Debug("RequestVote granted")
		task.reply.VoteFor = true

		f.resetTimer(logger)
		f.worker.state.UpdateTerm(peerTerm)
		f.worker.context.MarkStateChanged()
	}
}

func (f *Follower) HandleAppendEntriesTask(task *AppendEntriesTask) {
	currentTerm := f.worker.state.GetCurrentTerm()
	peerTerm := task.args.Term
	logger := f.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		handleTermBehindRequest(f.worker, task.reply, logger)
	} else {
		f.resetTimer(logger)
		f.worker.state.SyncStateFromAppendEntriesTask(task)
		f.worker.context.MarkStateChanged()
	}
}

func (f *Follower) HandleNotify() {
	if atomic.LoadInt32(&f.status) == timeout {
		f.worker.become(RoleCandidate)
	}
}

func (f *Follower) resetTimer(logger *zap.Logger) {
	logger.Debug("timer reset", zap.String("interval", f.worker.Timeout().String()))
	f.timer.Reset(f.worker.Timeout())
}

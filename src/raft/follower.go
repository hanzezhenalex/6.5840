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

	timer    *time.Timer
	stopCh   chan struct{}
	logger   *zap.Logger
	interval time.Duration

	status int32
}

func NewFollower(worker *Raft) *Follower {
	return &Follower{
		worker:   worker,
		stopCh:   make(chan struct{}),
		timer:    time.NewTimer(worker.interval),
		interval: worker.interval,
		status:   working,
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
	} else {
		logger.Debug("vote granted")
		task.reply.VoteFor = true
		f.worker.state.UpdateTerm(peerTerm)
	}
}

func (f *Follower) HandleAppendEntriesTask(task *AppendEntriesTask) {
	f.timer.Reset(f.interval)

	currentTerm := f.worker.state.GetCurrentTerm()
	peerTerm := task.args.Term
	logger := f.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		logger.Debug("AppendEntries reject, term ahead")
	} else {
		f.timer.Reset(f.interval)

		if currentTerm < peerTerm {
			f.worker.state.UpdateTerm(peerTerm)
		}

		// append entries
	}
}

func (f *Follower) HandleNotify() {
	if atomic.LoadInt32(&f.status) == timeout {
		f.worker.become(RoleCandidate)
	}
}

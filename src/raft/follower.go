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
	worker *Worker

	timer  *time.Timer
	stopCh chan struct{}
	logger *zap.Logger

	status int32
}

func NewFollower(worker *Worker) *Follower {
	return &Follower{
		worker: worker,
		stopCh: make(chan struct{}),
		timer:  time.NewTimer(worker.interval),
		status: working,
		logger: GetLoggerOrPanic("follower"),
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
			break LOOP
		}
	}

	f.timer.Stop()
}

func (f *Follower) StopDaemon() {
	close(f.stopCh)
}

func (f *Follower) Type() RoleType { return RoleFollower }

func (f *Follower) HandleRequestVotesTask(_ *RequestVotesTask) {}

func (f *Follower) HandleTickTask() {
	if atomic.LoadInt32(&f.status) == timeout {
		f.worker.become(RoleCandidate)
	}
}

func (f *Follower) HandleAppendEntries() {
	select {
	case <-f.stopCh:
	default:
		f.logger.Debug("flush interval")
		f.timer.Reset(f.worker.interval) // could be a bug, reset a closed timer?
	}
}

package raft

import (
	"6.5840/labrpc"
	"go.uber.org/zap"
)

type Leader struct {
	worker *Worker
	logger *zap.Logger
}

func NewLeader() *Leader {
	return &Leader{}
}

func (l *Leader) Type() RoleType {
	return RoleLeader
}

func (l *Leader) StartDaemon() {

}

func (l *Leader) StopDaemon() {

}

func (l *Leader) HandleTickTask() {}

func (l *Leader) HandleRequestVotesTask(_ *RequestVotesTask) {
	l.worker.become(RoleFollower)
}

type replicator struct {
	index  int
	logger *zap.Logger
	peer   *labrpc.ClientEnd
	log    *LogManager

	stopCh chan struct{}
}

func (rp *replicator) start() {

}

func (rp *replicator) stop() {
	close(rp.stopCh)
}

package raft

import (
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
}

func NewLeader(worker *Raft) *Leader {
	leader := &Leader{
		worker: worker,
		logger: GetLoggerOrPanic("leader").
			With(zap.Int(Term, worker.state.GetCurrentTerm())).
			With(zap.Int(Index, worker.me)),
		stopCh: make(chan struct{}),
		term:   worker.state.GetCurrentTerm(),
	}

	for index, p := range worker.peers {
		if index == worker.me {
			continue
		}
		leader.peers = append(leader.peers, NewReplicator(index, p, worker, leader.stopCh))
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
	} else {
		logger.Debug("vote granted")
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
		l.worker.state.UpdateTerm(peerTerm)

		// append entries
	}
}

type replicator struct {
	term      int
	me        int
	peerIndex int
	peer      *labrpc.ClientEnd

	logger    *zap.Logger
	log       *LogManager
	nextIndex int

	stopCh   chan struct{}
	interval time.Duration
	timeout  time.Duration
}

func NewReplicator(
	peerIndex int, peer *labrpc.ClientEnd, worker *Raft, stopCh chan struct{},
) *replicator {
	return &replicator{
		term:      worker.state.GetCurrentTerm(),
		me:        worker.me,
		peerIndex: peerIndex,
		peer:      peer,
		log:       NewLogManager(),
		logger: GetLoggerOrPanic("replicator").
			With(zap.Int(Peer, peerIndex)).
			With(zap.Int(Term, worker.state.GetCurrentTerm())).
			With(zap.Int(Index, worker.me)),
		stopCh:   stopCh,
		interval: worker.interval,
		timeout:  2 * time.Second,
	}
}

func (rp *replicator) daemon() {
	timer := time.NewTimer(rp.interval)

	for {
		select {
		case <-rp.stopCh:
			rp.logger.Info("replicator stopped")
			return
		case <-timer.C:
			rp.syncLogsWithPeer()
			timer.Reset(rp.interval)
		}
	}
}

func (rp *replicator) syncLogsWithPeer() {
	rp.logger.Info("replicator start sync")

	args := AppendEntryArgs{
		Term:          rp.term,
		Me:            rp.me,
		LastCommitted: rp.log.lastCommitted,
		LastLogIndex:  rp.log.lastLogIndex,
		LastLogTerm:   rp.log.lastLogTerm,
	}
	var reply AppendEntryReply

	if stopOnTimeout := rp.withTimeout(func() {
		rp.logger.Info("call RPC for sync logs")
		if ok := rp.peer.Call("Raft.AppendEntries", args, &reply); !ok {
			rp.logger.Error("fail to send RPC request to peer")
		}
	}); stopOnTimeout {
		rp.logger.Warn("timeout sending RPC request to peer")
		return
	}

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

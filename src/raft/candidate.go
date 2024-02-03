package raft

import (
	"sync/atomic"
	"time"

	"6.5840/labrpc"

	"go.uber.org/zap"
)

const (
	inElection      int32 = 0
	electionSucceed int32 = 1
	electionTimeout int32 = 2
)

type Candidate struct {
	worker *Raft

	currentElectionID int64 // atomic
	status            int32 // atomic
	currentTerm       int

	success chan int64
	stopCh  chan struct{}
	timer   *time.Timer
	logger  *zap.Logger
}

func NewCandidate(worker *Raft, interval time.Duration) *Candidate {
	return &Candidate{
		worker:      worker,
		success:     make(chan int64),
		stopCh:      make(chan struct{}),
		currentTerm: worker.state.GetCurrentTerm(),
		timer:       time.NewTimer(interval),
		status:      inElection,
		logger: GetLoggerOrPanic("candidate").
			With(zap.Int(Term, worker.state.GetCurrentTerm())).
			With(zap.Int(Index, worker.me)),
	}
}

func (c *Candidate) HandleNotify() {
	if atomic.LoadInt32(&c.status) == electionSucceed {
		c.worker.become(RoleLeader)
	} else if atomic.CompareAndSwapInt32(&c.status, electionTimeout, inElection) {
		c.worker.state.IncrTerm()
		c.currentTerm = c.worker.state.GetCurrentTerm()
		c.startNewSelection()
	}
}

func (c *Candidate) StartDaemon() {
	c.startNewSelection()

LOOP:
	for {
		select {
		case <-c.timer.C:
			atomic.CompareAndSwapInt32(&c.status, inElection, electionTimeout)
			c.worker.Notify(candidateTimeout)
		case <-c.stopCh:
			c.logger.Debug("shutdown")
			break LOOP
		case id := <-c.success:
			if id == atomic.LoadInt64(&c.currentElectionID) {
				c.logger.Info(
					"win selection",
					zap.Int64("id", id),
				)
				atomic.StoreInt32(&c.status, electionSucceed)
				c.worker.Notify(candidateBecomeLeader)
				break LOOP
			} else {
				c.logger.Info(
					"id mismatch, skip",
					zap.Int64("id", id),
				)
			}
		}
	}

	c.timer.Stop()
}

func (c *Candidate) StopDaemon() {
	close(c.stopCh)
}

func (c *Candidate) Type() RoleType {
	return RoleCandidate
}

func (c *Candidate) startNewSelection() {
	atomic.StoreInt64(&c.currentElectionID, time.Now().Unix())
	NewElection(
		c.currentElectionID,
		c.currentTerm,
		c.worker.me,
		c.success,
		c.worker.peers,
	).start()
}

func (c *Candidate) HandleRequestVotesTask(task *RequestVotesTask) {
	currentTerm := c.worker.state.GetCurrentTerm()
	peerTerm := task.args.Term
	logger := c.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		logger.Debug("RequestVote reject, term ahead")
		task.reply.VoteFor = false
	} else if currentTerm == peerTerm {
		logger.Debug("RequestVote reject, voted in this term")
		task.reply.VoteFor = false
	} else {
		c.worker.become(RoleFollower)
		c.worker.state.UpdateTerm(peerTerm)
		task.reply.VoteFor = true
	}
}

func (c *Candidate) HandleAppendEntriesTask(task *AppendEntriesTask) {
	currentTerm := c.worker.state.GetCurrentTerm()
	peerTerm := task.args.Term
	logger := c.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		logger.Debug("AppendEntries reject, term ahead")
	} else {
		logger.Info("someone has won the election")
		c.worker.become(RoleFollower)

		if currentTerm < peerTerm {
			c.worker.state.UpdateTerm(peerTerm)
		}

		// append entries
	}
}

type vote struct {
	id        int64
	voteForMe bool
}

type Election struct {
	id   int64
	term int
	me   int

	timer   *time.Timer
	result  chan vote
	success chan int64

	peers  []*labrpc.ClientEnd // RPC end points of all peers
	logger *zap.Logger
}

func NewElection(
	id int64,
	term int,
	me int,
	success chan int64,
	peers []*labrpc.ClientEnd,
) *Election {
	return &Election{
		id:      id,
		success: success,
		term:    term,
		me:      me,
		peers:   peers,
		result:  make(chan vote, len(peers)),
		logger: GetLoggerOrPanic("selection").
			With(zap.Int64("selection id", id)).
			With(zap.Int(Index, me)).
			With(zap.Int(Term, term)),
		timer: time.NewTimer(2 * time.Second),
	}
}

func (m *Election) start() {
	args := RequestVoteArgs{
		Term: m.term,
		Me:   m.me,
	}

	for index, peer := range m.peers {
		if index == m.me {
			continue
		}
		go m.call(index, peer, args)
	}

	go m.wait()
	m.logger.Info("selection started")
}

func (m *Election) newVote(voteFor bool) vote {
	return vote{
		id:        m.id,
		voteForMe: voteFor,
	}
}

func (m *Election) call(peerIndex int, peer *labrpc.ClientEnd, args RequestVoteArgs) {
	var reply RequestVoteReply
	logger := m.logger.With(zap.Int(Peer, peerIndex))

	logger.Debug("send rpc req for vote")
	if ok := peer.Call("Raft.RequestVote", args, &reply); !ok {
		logger.Warn("fail to send RPC to peer")
		reply.VoteFor = false
	}

	select {
	case m.result <- m.newVote(reply.VoteFor):
		logger.Debug("send vote result")
	case <-m.timer.C:
		logger.Debug("timeout, stop sending")
	}
}

func (m *Election) wait() {
	total := len(m.peers)
	half := total/2 + 1 // win -> more than half
	received := 0
	voted := 1 // vote for self at the beginning

	defer m.timer.Stop()

LOOP:
	for received < total-1 {
		select {
		case result := <-m.result:
			received++
			if result.voteForMe {
				voted++
			}
			m.logger.Debug(
				"receive one result",
				zap.Bool("vote for me", result.voteForMe),
				zap.Int("voted", voted),
				zap.Int("received", received),
				zap.Int("total", total),
			)
			if voted == half {
				m.logger.Info(
					"voted more than half, success",
					zap.Int("voted", voted),
					zap.Int("received", received),
					zap.Int("total", total),
				)
				m.success <- m.id
				break LOOP
			}
		case <-m.timer.C:
			m.logger.Debug("timeout, stop selection")
			return
		}
	}

	for received < total-1 {
		<-m.result
		m.logger.Debug(""+
			"vote has been succeed, clean up votes",
			zap.Int("received", received),
			zap.Int("total", total),
		)
		received++
	}
	m.logger.Debug("all vote result received")
}

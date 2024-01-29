package raft

import (
	"sync/atomic"
	"time"

	"6.5840/labrpc"

	"go.uber.org/zap"
)

const (
	inSelection int32 = 0
	succeed     int32 = 1
)

type Candidate struct {
	worker *Worker

	currentSelectionID int64
	succeed            int32
	currentTerm        int

	success chan int64
	stopCh  chan struct{}
	timer   *time.Timer
	logger  *zap.Logger
}

func NewCandidate(worker *Worker, interval time.Duration) *Candidate {
	return &Candidate{
		worker:      worker,
		success:     make(chan int64),
		stopCh:      make(chan struct{}),
		currentTerm: worker.state.term,
		timer:       time.NewTimer(interval),
		succeed:     inSelection,
		logger:      GetLoggerOrPanic("candidate"),
	}
}

func (c *Candidate) HandleRequestVotesTask(_ *RequestVotesTask) {
	c.worker.become(RoleFollower)
}

func (c *Candidate) HandleTickTask() {
	if atomic.LoadInt32(&c.succeed) == succeed {
		c.worker.become(RoleLeader)
	}
}

func (c *Candidate) StartDaemon() {
	c.startNewSelection()

LOOP:
	for {
		select {
		case <-c.timer.C:
			c.startNewSelection()
		case <-c.stopCh:
			c.logger.Debug("shutdown")
			break LOOP
		case id := <-c.success:
			if id == c.currentSelectionID {
				c.logger.Info(
					"win selection",
					zap.Int64("id", id),
				)
				atomic.StoreInt32(&c.succeed, succeed)
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
	c.currentSelectionID = time.Now().Unix()
	newSelection(
		c.currentSelectionID,
		c.currentTerm,
		c.worker.state.me,
		c.success,
		c.worker.peers,
	).start()
}

type vote struct {
	id        int64
	voteForMe bool
}

type selection struct {
	id   int64
	term int
	me   int

	timer   *time.Timer
	result  chan vote
	success chan int64

	peers  []*labrpc.ClientEnd // RPC end points of all peers
	logger *zap.Logger
}

func newSelection(
	id int64,
	term int,
	me int,
	success chan int64,
	peers []*labrpc.ClientEnd,
) *selection {
	return &selection{
		id:      id,
		success: success,
		term:    term,
		me:      me,
		peers:   peers,
		result:  make(chan vote, len(peers)),
		logger:  GetLoggerOrPanic("selection").With(zap.Int64("id", id)),
		timer:   time.NewTimer(10 * time.Second),
	}
}

func (m *selection) start() {
	args := RequestVoteArgs{
		Term: m.term,
		Me:   m.me,
	}

	for index, peer := range m.peers {
		go m.call(index, peer, &args)
	}

	go m.wait()
	m.logger.Info("selection started")
}

func (m *selection) newVote(voteFor bool) vote {
	return vote{
		id:        m.id,
		voteForMe: voteFor,
	}
}

func (m *selection) call(index int, peer *labrpc.ClientEnd, args *RequestVoteArgs) {
	var reply RequestVoteReply
	logger := m.logger.With(zap.Int("index", index))

	logger.Debug("send rpc req for vote")
	if ok := peer.Call("Raft.RequestVote", &args, &reply); !ok {
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

func (m *selection) wait() {
	total := len(m.peers)
	received := 0
	voted := 0

	defer m.timer.Stop()

LOOP:
	for received < total {
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
			)
			if voted > received/2 {
				m.logger.Info(
					"voted more than half, success",
					zap.Int("voted", voted),
					zap.Int("received", received),
				)
				m.success <- m.id
				break LOOP
			}
		case <-m.timer.C:
			m.logger.Debug("timeout, stop selection")
			break LOOP
		}
	}
}

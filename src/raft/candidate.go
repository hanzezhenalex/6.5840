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
	timeout           time.Duration

	success chan int64
	stopCh  chan struct{}
	timer   *time.Timer
	logger  *zap.Logger
}

func NewCandidate(worker *Raft) *Candidate {
	return &Candidate{
		worker:  worker,
		success: make(chan int64),
		stopCh:  make(chan struct{}),
		timer:   time.NewTimer(worker.timeout),
		timeout: worker.timeout,
		status:  inElection,
		logger: GetLoggerOrPanic("candidate").
			With(zap.Int(Index, worker.me)),
	}
}

func (c *Candidate) HandleNotify() {
	if atomic.LoadInt32(&c.status) == electionSucceed {
		c.worker.become(RoleLeader)
	} else if atomic.CompareAndSwapInt32(&c.status, electionTimeout, inElection) {
		c.worker.state.IncrTerm()
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
	go NewElection(
		c.currentElectionID,
		c.worker.state.GetCurrentTerm(),
		c.worker.me,
		c.worker.state.logMngr.GetLastLogIndex(),
		c.worker.state.logMngr.GetLastLogTerm(),
		c.success,
		c.worker.peers,
	).start()
	c.timer.Reset(c.timeout)
}

func (c *Candidate) HandleRequestVotesTask(task *RequestVotesTask) {
	currentTerm := c.worker.state.GetCurrentTerm()
	peerTerm := task.args.Term
	logger := c.logger.With(
		zap.Int(Peer, task.args.Me),
		zap.Int(PeerTerm, task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		logger.Debug("RequestVote reject, term ahead")
		task.reply.VoteFor = false
	} else if currentTerm == peerTerm {
		logger.Debug("RequestVote reject, voted in this term")
		task.reply.VoteFor = false
	} else {
		c.worker.state.UpdateTerm(peerTerm)

		if c.worker.state.IsLogAheadPeer(
			task.args.LeaderLastLogIndex, task.args.LeaderLastLogTerm,
		) {
			logger.Debug("RequestVote reject, log ahead peer",
				zap.Int("lastLogIndex", c.worker.state.logMngr.GetLastLogIndex()),
				zap.Int("lastLogTerm", c.worker.state.logMngr.GetLastLogTerm()),
				zap.Int("peerLastLogIndex", task.args.LeaderLastLogIndex),
				zap.Int("peerLastLogTerm", task.args.LeaderLastLogTerm),
			)
			task.reply.VoteFor = false
			c.worker.become(RoleCandidate)
		} else {
			logger.Debug("RequestVote granted")
			c.worker.become(RoleFollower)
			task.reply.VoteFor = true
		}
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
		handleTermBehindRequest(c.worker, task.reply, logger)
	} else {
		logger.Info("someone has won the election")
		c.worker.become(RoleFollower)
		c.worker.state.SyncStateFromAppendEntriesTask(task)
	}
}

type vote struct {
	id        int64
	voteForMe bool
}

type Election struct {
	id   int64
	args RequestVoteArgs
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
	leaderLastLogIndex int,
	leaderLastLogTerm int,
	success chan int64,
	peers []*labrpc.ClientEnd,
) *Election {
	return &Election{
		id:      id,
		success: success,
		args: RequestVoteArgs{
			Term:               term,
			Me:                 me,
			LeaderLastLogTerm:  leaderLastLogTerm,
			LeaderLastLogIndex: leaderLastLogIndex,
		},
		me:     me,
		peers:  peers,
		result: make(chan vote, len(peers)),
		logger: GetLoggerOrPanic("selection").
			With(zap.Int64("selection id", id)).
			With(zap.Int(Index, me)).
			With(zap.Int(Term, term)),
		timer: time.NewTimer(2 * time.Second),
	}
}

func (m *Election) start() {
	for index, peer := range m.peers {
		if index == m.me {
			continue
		}
		go m.call(index, peer, m.args)
	}

	m.logger.Info("selection started")
	m.wait()
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

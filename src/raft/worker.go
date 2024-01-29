package raft

import (
	"math/rand"
	"time"

	"6.5840/labrpc"

	"go.uber.org/zap"
)

type RoleType string

const (
	RoleCandidate = "candidate"
	RoleLeader    = "leader"
	RoleFollower  = "follower"
)

type Role interface {
	StartDaemon()
	StopDaemon()
	Type() RoleType
	HandleRequestVotesTask(task *RequestVotesTask)
	HandleTickTask()
	//HandleAppendEntriesTask(task *AppendEntriesTask)
}

type StateManager struct {
	me   int
	term int
}

func (state *StateManager) updateIfBehindPeer(peerTerm int) bool {
	if state.term < peerTerm {
		// update term whenever we found a newer term
		state.term = peerTerm
		return true
	}
	return false
}

type Worker struct {
	state *StateManager // raft state
	mngr  *LogManager

	role   Role
	logger *zap.Logger

	peers    []*labrpc.ClientEnd // RPC end points of all peers
	interval time.Duration
}

func NewWorker() *Worker {
	worker := &Worker{
		interval: time.Duration(50+(rand.Int63()%300)) * time.Millisecond,
	}
	return worker
}

type RequestVotesTask struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

func (w *Worker) HandleRequestVotesTask(task *RequestVotesTask) {
	if w.state.updateIfBehindPeer(task.args.Term) {
		w.role.HandleRequestVotesTask(task)
		task.reply.VoteFor = true
	} else {
		w.logger.Debug(
			"RequestVote reject",
			zap.Int("peer term", task.args.Term),
			zap.Int("my term", w.state.term),
		)
		task.reply.VoteFor = false
	}
	task.reply.Term = w.state.term
}

type AppendEntriesTask struct {
}

func (w *Worker) become(role RoleType) {
	AssertTrue(
		w.role.Type() == role,
		"can not transform to the same role, role=%s", role,
	)

	w.role.StopDaemon()

	switch role {
	case RoleFollower:
		w.role = NewFollower(w)
	case RoleLeader:
		w.role = NewLeader()
	case RoleCandidate:
		w.state.term++
		w.role = NewCandidate(w, w.interval)
	}

	go w.role.StartDaemon()
}

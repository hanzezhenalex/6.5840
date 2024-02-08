package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"

	"go.uber.org/zap"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

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
	HandleNotify()
	HandleAppendEntriesTask(task *AppendEntriesTask)
	HandleRequestVotesTask(task *RequestVotesTask)
}

type Raft struct {
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	state     *StateManager

	role   Role
	logger *zap.Logger

	peers             []*labrpc.ClientEnd // RPC end points of all peers
	timeout           time.Duration
	heartBeatInterval time.Duration

	stopCh            chan struct{}
	appendEntriesCh   chan *AppendEntriesTask
	requestVoteCh     chan *RequestVotesTask
	getStateCh        chan *StateTask
	storeNewCommandCh chan *StoreNewCommandTask
	notifyCh          chan struct{}
	commitTicker      *time.Ticker

	lastCommitted int
	applyMsgCh    chan ApplyMsg
}

func Make(peers []*labrpc.ClientEnd, me int,
	persistent *Persister, applyCh chan ApplyMsg) *Raft {
	worker := &Raft{
		me: me,
		state: &StateManager{
			committed: -1,
			term:      0,
			logMngr:   NewLogManager(me),
		},
		peers: peers,
		logger: GetLoggerOrPanic("raft").
			With(zap.Int(Index, me)),
		timeout:           time.Duration(150+(rand.Int63()%150)) * time.Millisecond,
		heartBeatInterval: time.Duration(100) * time.Millisecond,

		stopCh:            make(chan struct{}),
		appendEntriesCh:   make(chan *AppendEntriesTask),
		requestVoteCh:     make(chan *RequestVotesTask),
		getStateCh:        make(chan *StateTask),
		storeNewCommandCh: make(chan *StoreNewCommandTask),
		notifyCh:          make(chan struct{}),
		commitTicker:      time.NewTicker(200 * time.Millisecond),

		applyMsgCh:    applyCh,
		lastCommitted: EmptyLogIndex,
	}
	worker.role = NewFollower(worker)

	// initialize from state persisted before a crash
	worker.readPersist(persistent.ReadRaftState())

	go worker.daemon()
	return worker
}

func (rf *Raft) GetState() (int, bool) {
	task := StateTask{ch: make(chan State)}
	rf.getStateCh <- &task
	select {
	case <-rf.stopCh:
		panic(errorWorkerStopped)
	case state := <-task.ch:
		return state.term, state.isLeader
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	log := &LogEntry{
		Index:   -1,
		Term:    -1,
		Command: command,
	}
	task := &StoreNewCommandTask{
		Log: log,
	}
	task.wg.Add(1)

	select {
	case <-rf.stopCh:
		return -1, -1, false
	case rf.storeNewCommandCh <- task:
	}

	task.wg.Wait()
	rf.logger.Info(
		"store new command",
		zap.Bool("success", task.Success),
		zap.String("command", fmt.Sprintf("%#v", log)),
	)
	return log.Index, log.Term, task.Success
}

func (rf *Raft) Kill() { close(rf.stopCh) }

type StoreNewCommandTask struct {
	Success bool
	Log     *LogEntry
	wg      sync.WaitGroup
}

func (rf *Raft) handleStoreNewCommandTask(task *StoreNewCommandTask) {
	defer task.wg.Done()

	if rf.role.Type() == RoleLeader {
		rf.state.logMngr.AppendNewLog(rf.state.GetCurrentTerm(), task.Log.Command)
		rf.role.(*Leader).UpdateReplicatorState()

		task.Log.Term = rf.state.GetCurrentTerm()
		task.Log.Index = rf.state.logMngr.GetLastLogIndex()

		task.Success = true
	} else {
		task.Success = false
	}
}

type State struct {
	term     int
	isLeader bool
}

type StateTask struct {
	ch chan State
}

func (rf *Raft) handleGetStateTask() State {
	return State{
		term:     rf.state.GetCurrentTerm(),
		isLeader: rf.role.Type() == RoleLeader,
	}
}

type RequestVotesTask struct {
	args  RequestVoteArgs
	reply *RequestVoteReply
	wg    sync.WaitGroup
}

func (rf *Raft) handleRequestVotesTask(task *RequestVotesTask) {
	defer func() {
		task.reply.Term = rf.state.GetCurrentTerm()
		task.wg.Done()
	}()
	rf.role.HandleRequestVotesTask(task)
}

type AppendEntriesTask struct {
	args  AppendEntryArgs
	reply *AppendEntryReply
	wg    sync.WaitGroup
}

func (rf *Raft) handleAppendEntriesTask(task *AppendEntriesTask) {
	defer func() {
		task.reply.Term = rf.state.GetCurrentTerm()
		task.wg.Done()
	}()
	rf.role.HandleAppendEntriesTask(task)
}

func (rf *Raft) daemon() {
	rf.logger.Info("daemon started")
	go rf.role.StartDaemon()

LOOP:
	for {
		select {
		case <-rf.stopCh:
			break LOOP
		case task := <-rf.appendEntriesCh:
			rf.handleAppendEntriesTask(task)
		case task := <-rf.requestVoteCh:
			rf.handleRequestVotesTask(task)
		case <-rf.notifyCh:
			rf.role.HandleNotify()
		case task := <-rf.getStateCh:
			task.ch <- rf.handleGetStateTask()
		case task := <-rf.storeNewCommandCh:
			rf.handleStoreNewCommandTask(task)
		case <-rf.commitTicker.C:
			rf.apply()
		}
	}

	rf.role.StopDaemon()
}

func (rf *Raft) apply() {
	if rf.role.Type() == RoleLeader {
		rf.role.(*Leader).UpdateCommittedIndex()
	}

	oldLastCommitted := rf.lastCommitted

LOOP:
	for i := rf.lastCommitted + 1; i <= rf.state.committed; i++ {
		log, err := rf.state.logMngr.GetLogByIndex(i)
		if err != nil {
			break LOOP
		}
		rf.applyMsgCh <- ApplyMsg{
			Command:      log.Command,
			CommandValid: true,
			CommandIndex: log.Index,
		}
		rf.lastCommitted++
	}

	if oldLastCommitted != rf.lastCommitted {
		rf.logger.Info(
			"log committed",
			zap.Int("old", oldLastCommitted),
			zap.Int("current", rf.lastCommitted),
		)
	}
}

func (rf *Raft) become(role RoleType) {
	if rf.role.Type() == role && rf.role.Type() != RoleCandidate {
		panic(fmt.Errorf("can not transform to the same role, role=%s", role))
	}

	rf.logger.Info(
		"role changed",
		zap.String("from", string(rf.role.Type())),
		zap.String("to", string(role)),
	)

	rf.role.StopDaemon()

	switch role {
	case RoleFollower:
		rf.role = NewFollower(rf)
	case RoleLeader:
		rf.role = NewLeader(rf)
	case RoleCandidate:
		rf.state.IncrTerm()
		rf.role = NewCandidate(rf)
	}

	go rf.role.StartDaemon()
}

func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) error {
	task := &AppendEntriesTask{args: args, reply: reply}
	task.wg.Add(1)

	select {
	case <-rf.stopCh:
		return errorWorkerStopped
	case rf.appendEntriesCh <- task:
		rf.logger.Debug("put AppendEntriesTask")
	}

	task.wg.Wait()
	return nil
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	task := &RequestVotesTask{args: args, reply: reply}
	task.wg.Add(1)

	select {
	case <-rf.stopCh:
		return errorWorkerStopped
	case rf.requestVoteCh <- task:
		rf.logger.Debug("put RequestVotesTask")
	}

	task.wg.Wait()
	return nil
}

func (rf *Raft) Notify(msg string) {
	rf.logger.Debug("notify worker", zap.String("reason", msg))
	go func() { rf.notifyCh <- struct{}{} }()
}

func handleTermBehindRequest(worker *Raft, reply *AppendEntryReply, logger *zap.Logger) {
	logger.Debug("AppendEntries reject, term ahead")
	reply.Term = worker.state.GetCurrentTerm()
	reply.ExpectedNextIndex = worker.state.logMngr.GetLastLogIndex()
}

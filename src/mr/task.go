package mr

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Result map[string]interface{}

func (res Result) StringSlice() []string {
	ret := make([]string, 0, len(res))
	for _, val := range res {
		ret = append(ret, val.(string))
	}
	return ret
}

type inmemoryBatch struct {
	id                string
	redeliverInterval time.Duration
	logger            *zap.Logger

	wg sync.WaitGroup // for safety close

	processing []*Record
	completed  map[string]interface{}
	pending    []*Record
}

func newBatch(id string, redeliverInterval time.Duration, req *requestPutBatch) (*inmemoryBatch, error) {
	batch := &inmemoryBatch{
		id:                id,
		completed:         make(map[string]interface{}),
		redeliverInterval: redeliverInterval,
	}

	if err := batch.initLogger(); err != nil {
		return nil, fmt.Errorf("fail to init logger %w", err)
	}
	for n, d := range req.desc {
		batch.pending = append(batch.pending, &Record{
			task: &Task{
				TaskHeader: TaskHeader{
					BatchID: batch.id,
					ID:      fmt.Sprintf("task-%s-%d", batch.id, n),
				},
				Desc: d,
			},
		})
	}
	batch.wg.Add(1)

	return batch, nil
}

func (batch *inmemoryBatch) initLogger() error {
	logger, err := GetBaseLogger()
	if err != nil {
		return err
	}
	batch.logger = logger.With(
		zap.String(LoggerComponent, "inmemoryBatch"),
		zap.String("id", batch.id),
	)
	return nil
}

func (batch *inmemoryBatch) Wait() Result {
	batch.wg.Wait()
	return batch.completed
}

func (batch *inmemoryBatch) done() {
	batch.wg.Done()
	_ = batch.logger.Sync()
}

func (batch *inmemoryBatch) deliverPendingFront() *Task {
	record := batch.pending[0]
	batch.pending = batch.pending[1:]

	record.deliverAt = time.Now()
	batch.processing = append(batch.processing, record)

	batch.logger.Info("deliver from pending queue",
		zap.String("task id", record.task.ID))
	return record.task
}

func (batch *inmemoryBatch) tryDeliverProcessingFront() *Task {
	record := batch.processing[0]
	now := time.Now()

	if record.deliverAt.Add(batch.redeliverInterval).After(now) {
		record.deliverAt = now
		batch.processing = append(batch.processing[1:], record)
		batch.logger.Info("deliver one task from processing queue",
			zap.String("task id", record.task.ID))
		return record.task
	}

	batch.logger.Info("no task delivered from processing queue")
	return nil
}

func (batch *inmemoryBatch) deliverOne() (*Task, bool) {
	batch.cleanup()

	if len(batch.pending) > 0 {
		return batch.deliverPendingFront(), false
	}
	if len(batch.processing) > 0 {
		return batch.tryDeliverProcessingFront(), false
	}

	batch.logger.Info("current batch is completed")
	batch.done()
	return nil, true
}

func (batch *inmemoryBatch) cleanup() {
	n := 0
	for ; n < len(batch.processing); n++ {
		record := batch.processing[n]
		if record == nil {
			continue
		}
		if _, ok := batch.completed[record.task.ID]; ok {
			continue
		}
		break
	}
	batch.processing = batch.processing[n:]
}

func (batch *inmemoryBatch) complete(id string, output interface{}) {
	if _, ok := batch.completed[id]; !ok {
		batch.completed[id] = output
		batch.cleanup()
	}
}

func (batch *inmemoryBatch) fail(id string) {
	for n, record := range batch.processing {
		if record.task.ID == id {
			batch.processing[n] = nil
			batch.pending = append(batch.pending, record)
			return
		}
	}
}

var (
	errBatchStillRunning = errors.New("there is batch running")
	errMngrStopped       = errors.New("mngr has stopped running")
)

type BatchTaskManager interface {
	Put(desc []interface{}) (*inmemoryBatch, error)
	Take() *Task
	ReportSuccess(header TaskHeader, output interface{})
	ReportFailure(header TaskHeader)
	Close()
}

const (
	interval       = time.Second
	running  int32 = 1
	closed   int32 = 0
)

type TaskHeader struct {
	BatchID string
	ID      string
}

type Task struct {
	TaskHeader
	Desc interface{}
}

func (task *Task) copy() *Task {
	var duplicated Task
	duplicated = *task // shallow copy, do not edit Desc
	return &duplicated
}

type Record struct {
	task      *Task
	deliverAt time.Time
}

type requestNewJob struct {
	ch chan *Task
}

type requestPutBatch struct {
	desc []interface{}
	ch   chan *inmemoryBatch
}

type reportTaskStatus struct {
	TaskHeader
	output interface{}
}

type singleBatchManager struct {
	currentBatch      *inmemoryBatch
	redeliverInterval time.Duration
	logger            *zap.Logger

	putNewBatchCh chan *requestPutBatch
	requestCh     chan *requestNewJob
	reportSuccess chan reportTaskStatus
	reportFailure chan TaskHeader

	ticker *time.Ticker

	queue []*requestNewJob

	stopCh chan struct{}
	closed int32
	wg     sync.WaitGroup // for safety close
}

func NewInMemoryManager(threshold time.Duration) *singleBatchManager {
	mngr := &singleBatchManager{
		redeliverInterval: threshold,

		putNewBatchCh: make(chan *requestPutBatch),
		requestCh:     make(chan *requestNewJob),
		reportSuccess: make(chan reportTaskStatus),
		reportFailure: make(chan TaskHeader),

		ticker: time.NewTicker(interval),

		stopCh: make(chan struct{}),
		closed: running,
	}
	_ = mngr.initLogger()

	go mngr.daemon()
	return mngr
}

func (mngr *singleBatchManager) initLogger() error {
	logger, err := GetBaseLogger()
	if err != nil {
		return err
	}
	mngr.logger = logger.With(
		zap.String(LoggerComponent, "singleBatchManager"),
	)
	return nil
}

func (mngr *singleBatchManager) Take() *Task {
	req := &requestNewJob{
		ch: make(chan *Task),
	}

	select {
	case <-mngr.stopCh:
		return nil
	case mngr.requestCh <- req:
		return <-req.ch
	}
}

func (mngr *singleBatchManager) ReportSuccess(header TaskHeader, output interface{}) {

	select {
	case mngr.reportSuccess <- reportTaskStatus{
		header,
		output,
	}:
	case <-mngr.stopCh:
	}
}

func (mngr *singleBatchManager) ReportFailure(header TaskHeader) {

	select {
	case mngr.reportFailure <- header:
	case <-mngr.stopCh:
	}
}

func (mngr *singleBatchManager) Close() {
	success := atomic.CompareAndSwapInt32(&mngr.closed, running, closed)
	if success {
		close(mngr.stopCh)
	}
	mngr.wg.Wait()
	_ = mngr.logger.Sync()
}

func (mngr *singleBatchManager) Put(desc []interface{}) (*inmemoryBatch, error) {
	req := &requestPutBatch{
		desc: desc,
		ch:   make(chan *inmemoryBatch),
	}

	select {
	case <-mngr.stopCh:
		return nil, errMngrStopped
	case mngr.putNewBatchCh <- req:
		batch := <-req.ch
		if batch == nil {
			return nil, errBatchStillRunning
		}
		return batch, nil
	}
}

func (mngr *singleBatchManager) tryDeliverInCurrentBatch() *Task {
	if mngr.currentBatch == nil {
		return nil
	}

	task, batchClosed := mngr.currentBatch.deliverOne()
	if batchClosed {
		mngr.currentBatch = nil
	}
	return task
}

func (mngr *singleBatchManager) tryDeliverReqInQueue() {
	nextDeliver := 0
LOOP:
	for ; nextDeliver < len(mngr.queue); nextDeliver++ {
		if mngr.currentBatch == nil {
			break LOOP
		}
		task := mngr.tryDeliverInCurrentBatch()
		if task == nil {
			break LOOP
		}
		req := mngr.queue[nextDeliver]
		req.ch <- task.copy()
	}

	if nextDeliver > 0 {
		mngr.queue = mngr.queue[nextDeliver:]
		mngr.logger.Info("req tasks delivered", zap.Int("delivered", len(mngr.queue)))
	}
}

func (mngr *singleBatchManager) createNewBatch(req *requestPutBatch) {
	batch, err := newBatch(fmt.Sprintf("%d", time.Now().Unix()), mngr.redeliverInterval, req)

	if err != nil {
		mngr.logger.Error("fail to create new batch", zap.Error(err))
	}
	mngr.currentBatch = batch
	mngr.logger.Info("new batch started",
		zap.String("batch id", mngr.currentBatch.id))
}

func (mngr *singleBatchManager) daemon() {
	mngr.wg.Add(1)
	defer mngr.wg.Done()

LOOP:
	for {
		select {
		case req := <-mngr.putNewBatchCh:
			if mngr.currentBatch != nil {
				mngr.logger.Info("putNewBatch rejected, batch is running")
				req.ch <- nil
			}
			mngr.createNewBatch(req)
			req.ch <- mngr.currentBatch
		case req := <-mngr.requestCh:
			mngr.logger.Info("receive new request")
			task := mngr.tryDeliverInCurrentBatch()
			if task != nil {
				mngr.logger.Info("task delivered", zap.String("id", task.ID))
				req.ch <- task
			} else {
				mngr.queue = append(mngr.queue, req)
				mngr.logger.Info("no available task, in queue", zap.Int("queue size", len(mngr.queue)))
			}
		case <-mngr.ticker.C:
			mngr.logger.Debug("tick")
			mngr.tryDeliverReqInQueue()
		case header := <-mngr.reportSuccess:
			mngr.logger.Info("task success",
				zap.String("task id", header.ID),
				zap.String("batch id", header.BatchID))
			mngr.complete(header)
		case header := <-mngr.reportFailure:
			mngr.logger.Info("task failed",
				zap.String("task id", header.ID),
				zap.String("batch id", header.BatchID))
			mngr.fail(header)
			mngr.tryDeliverReqInQueue()
		case <-mngr.stopCh:
			break LOOP
		}
	}

	// clean up queue
	for _, req := range mngr.queue {
		req.ch <- nil
	}
	mngr.ticker.Stop()
}

func (mngr *singleBatchManager) complete(report reportTaskStatus) {
	if mngr.currentBatch == nil || report.BatchID != mngr.currentBatch.id {
		return
	}
	mngr.currentBatch.complete(report.ID, report.output)
}

func (mngr *singleBatchManager) fail(header TaskHeader) {
	if mngr.currentBatch == nil || header.BatchID != mngr.currentBatch.id {
		return
	}
	mngr.currentBatch.fail(header.ID)
}

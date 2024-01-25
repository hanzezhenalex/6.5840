package mr

import (
	"fmt"
	"go.uber.org/zap"
	"log"
	"sort"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type workingStage int32

func (ws workingStage) string() string {
	switch ws {
	case stagePending:
		return "pending"
	case stageReducing:
		return "reducing"
	case stageMapping:
		return "mapping"
	case stageDone:
		return "done"
	case stageShuffling:
		return "shuffling"
	default:
		return "unknown"
	}
}

const (
	stagePending workingStage = iota + 1
	stageMapping
	stageReducing
	stageShuffling
	stageDone
)

type Coordinator struct {
	store          JsonStore
	stage          workingStage
	lastInstanceID int32
	logger         *zap.Logger

	taskMngr BatchTaskManager
	shuffler Shuffler

	reqeustCh chan *requestNewJob
	reportCh  chan *Job
}

// server start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	RegisterGobStruct()
	if err := rpc.Register(c); err != nil {
		panic(err)
	}
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%s", Port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() { _ = http.Serve(l, nil) }()
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.inStage(stageDone)
}

func (c *Coordinator) inStage(stage workingStage) bool {
	return atomic.LoadInt32((*int32)(&c.stage)) == int32(stage)
}

func (c *Coordinator) setStage(stage workingStage) {
	c.logger.Info(
		"start new stage",
		zap.String("new", stage.string()),
		zap.String("old", c.stage.string()),
	)
	atomic.StoreInt32((*int32)(&c.stage), int32(stage))
}

func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	if args.InstanceID == InitInstanceID {
		c.onboardNewInstance(reply)
	} else {
		reply.InstanceID = args.InstanceID
		if args.Job.Status == StatusJobFailed {
			c.taskMngr.ReportFailure(args.Job.TaskHeader)
		} else if args.Job.Status == StatusJobSucceed {
			c.taskMngr.ReportSuccess(args.Job.TaskHeader, args.Job.Output)
		} else {
			c.logger.Error("incorrect job status", zap.String("status", string(args.Job.Status)))
		}
	}

	task := c.taskMngr.Take()

	if task == nil {
		reply.Job = &Job{
			Status: StatusJobDelivery,
			JobDesc: JobDesc{
				JobType: JobNoJob,
			},
		}
	} else {
		reply.Job = &Job{
			Status:  StatusJobDelivery,
			Task:    *task,
			JobDesc: task.Desc.(JobDesc),
		}
	}
	return nil
}

func (c *Coordinator) onboardNewInstance(reply *RequestJobReply) {
	reply.InstanceID = atomic.AddInt32(&c.lastInstanceID, 1)
}

func (c *Coordinator) createJobs(inputs []string, jobTye JobType) []interface{} {
	var ret []interface{}
	for _, input := range inputs {
		ret = append(ret, JobDesc{
			JobType: jobTye,
			Input:   input,
		})
	}
	return ret
}

func (c *Coordinator) run(input []string) {
	c.setStage(stageMapping)

	mapBatch, err := c.taskMngr.Put(c.createJobs(input, JobMap))
	if err != nil {
		panic(err)
	}
	outputs := mapBatch.Wait().StringSlice()
	if err != nil {
		panic(err)
	}

	c.setStage(stageShuffling)
	outputs, err = c.shuffler.Shuffle(outputs)
	if err != nil {
		panic(err)
	}

	c.setStage(stageReducing)
	reduceBatch, err := c.taskMngr.Put(c.createJobs(outputs, JobReduce))
	if err != nil {
		panic(err)
	}

	outputs = reduceBatch.Wait().StringSlice()
	if err = c.write(outputs); err != nil {
		panic(err)
	}

	c.setStage(stageDone)
}

func (c *Coordinator) write(files []string) error {
	var rets []string
	for _, file := range files {
		kv, err := c.store.RetrieveKV(file)
		if err != nil {
			return fmt.Errorf("fail to retrieve kv, file=%s, %w", file, err)
		}
		rets = append(rets, fmt.Sprintf("%s %s\r\n", kv[0].Key, kv[0].Value))
	}

	sort.Slice(rets, func(i, j int) bool {
		return rets[i] < rets[j]
	})

	f, err := os.Create("mr-out-1")
	if err != nil {
		return fmt.Errorf("fail to create mr-out, %w", err)
	}
	defer func() { _ = f.Close() }()

	for _, ret := range rets {
		if _, err := f.WriteString(ret); err != nil {
			return fmt.Errorf("fail to write to mr-out, content=%s, %w", ret, err)
		}
	}
	return nil
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce processing to use.
func MakeCoordinator(files []string, _ int) *Coordinator {
	c := Coordinator{
		store:          NewJsonStore(""),
		stage:          stagePending,
		lastInstanceID: 0,
		taskMngr:       NewInMemoryManager(10 * time.Second),
		reqeustCh:      make(chan *requestNewJob),
		reportCh:       make(chan *Job),
	}

	logger, err := GetBaseLogger()
	if err != nil {
		panic(fmt.Errorf("fail to get base logger: %w", err))
	}
	c.logger = logger.With(zap.String(LoggerComponent, "coordinator"))

	shuffler, err := NewInMemoryShuffler("")
	if err != nil {
		panic(fmt.Errorf("fail to create shuffler: %w", err))
	}
	c.shuffler = shuffler

	go c.run(files)
	c.server()
	return &c
}

package mr

import (
	"fmt"
	"go.uber.org/zap"
	"time"
)
import "log"
import "net/rpc"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	RegisterGobStruct()

	param := &RPCParam{
		InstanceID: InitInstanceID,
	}
	worker := &worker{
		store:   NewJsonStore(""),
		logger:  GetLoggerOrPanic("worker"),
		prefix:  fmt.Sprintf("%d", time.Now().Unix()),
		mapf:    mapf,
		reducef: reducef,
	}

	worker.logger.Info("worker started")

	for {
		if call(
			"Coordinator.RequestJob",
			(*RequestJobArgs)(param),
			(*RequestJobReply)(param),
		) == false {
			worker.logger.Error("fail to call RPC")
			return
		}

		switch param.Job.JobType {
		case JobNoJob:
			worker.logger.Info("no job, shutdown")
			return
		default:
			output, err := worker.handle(param)
			if err != nil {
				param.Job.Status = StatusJobFailed
				worker.logger.Error(
					"fail to handle job",
					zap.String("id", fmt.Sprintf("%s-%s", param.Job.BatchID, param.Job.ID)),
					zap.Error(err),
				)
			} else {
				worker.logger.Info(
					"process succeed",
					zap.String("id", fmt.Sprintf("%s-%s", param.Job.BatchID, param.Job.ID)),
				)
				param.Job.Status = StatusJobSucceed
				param.Job.Output = output
			}
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:"+Port)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func() { _ = c.Close() }()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

type worker struct {
	count      int
	prefix     string
	instanceId int32
	store      JsonStore
	logger     *zap.Logger

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) validate(param *RPCParam) error {
	if param.Job.Status != StatusJobDelivery {
		return fmt.Errorf("incorrect job status, got %s", param.Job.Status)
	}
	w.instanceId = param.InstanceID
	w.count++
	return nil
}

func (w *worker) filename(job *Job) string {
	return fmt.Sprintf("%s_%s_%s_%d_%d.json", job.JobType, job.BatchID, job.ID, w.instanceId, w.count)
}

func (w *worker) handle(param *RPCParam) (string, error) {
	if err := w.validate(param); err != nil {
		return "", fmt.Errorf("fail to validate param, %w", err)
	}

	switch param.Job.JobType {
	case JobMap:
		return w.handleMapJob(param)
	case JobReduce:
		return w.handleReduceJob(param)
	default:
		return "", fmt.Errorf("unknown job type, got %s", param.Job.JobType)
	}
}

func (w *worker) handleMapJob(param *RPCParam) (string, error) {
	input := param.Job.JobDesc.Input

	raw, err := w.store.readFile(param.Job.Input)
	if err != nil {
		return "", fmt.Errorf("fail to read input, %w", err)
	}

	kvs := w.mapf(input, string(raw))
	output := w.filename(param.Job)
	if err := w.store.StoreKV(output, kvs); err != nil {
		return "", fmt.Errorf("fail to store kv, %w", err)
	}
	return output, nil
}

func (w *worker) handleReduceJob(param *RPCParam) (string, error) {
	input := param.Job.JobDesc.Input

	rets, err := w.store.RetrieveShufflingBatch(input)
	if err != nil {
		return "", fmt.Errorf("fail to read input, %w", err)
	}

	var reduceResults []KeyValue
	for _, ret := range rets {
		reduceResults = append(reduceResults, KeyValue{
			Key:   ret.Key,
			Value: w.reducef(ret.Key, ret.Values),
		})
	}

	output := w.filename(param.Job)
	if err := w.store.StoreKV(output, reduceResults); err != nil {
		return "", fmt.Errorf("fail to store output, %w", err)
	}
	return output, nil
}

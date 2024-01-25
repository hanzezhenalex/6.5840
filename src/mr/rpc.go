package mr

import "encoding/gob"

//
// RPC definitions.
//
// remember to capitalize all names.
//

type JobType string

const (
	JobMap    JobType = "map"
	JobReduce JobType = "reduce"
	JobNoJob  JobType = "no job"
)

type JobStatus string

const (
	StatusJobDelivery JobStatus = "delivery"
	StatusJobSucceed  JobStatus = "succeed"
	StatusJobFailed   JobStatus = "failed"
)

type JobDesc struct {
	JobType JobType
	Input   string
}

type Job struct {
	Task
	JobDesc
	Output string
	Status JobStatus
}

type RequestJobArgs RPCParam
type RequestJobReply RPCParam

// RPCParam is for args & reply
type RPCParam struct {
	InstanceID int32
	Job        *Job
}

const Port = "9001"

func RegisterGobStruct() {
	gob.Register(JobDesc{})
	gob.Register(Task{})
	gob.Register(TaskHeader{})
	gob.Register(Job{})
}

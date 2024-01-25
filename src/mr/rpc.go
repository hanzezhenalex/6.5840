package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

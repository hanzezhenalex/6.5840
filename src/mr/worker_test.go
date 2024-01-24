package mr

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mockMapf(string, string) []KeyValue {
	panic("not implement")
}

func mockReduceFn(string, []string) string {
	panic("not implement")
}

func Test_worker(t *testing.T) {
	rq := require.New(t)
	worker := &worker{
		store:   NewJsonStore(filepath.Join(testFolder, "worker")),
		logger:  GetLoggerOrPanic("worker"),
		prefix:  fmt.Sprintf("%d", time.Now().Unix()),
		mapf:    mockMapf,
		reducef: mockReduceFn,
	}

	t.Run("mapf", func(t *testing.T) {
		const input = "work-map-1.txt"

		worker.mapf = func(key string, val string) []KeyValue {
			rq.Equal(input, key)
			rq.Equal(0, len(val))

			return []KeyValue{
				{
					Key:   "1",
					Value: "1",
				},
			}
		}
		defer func() {
			worker.mapf = mockMapf
		}()

		output, err := worker.handleMapJob(&RPCParam{
			InstanceID: 100,
			Job: &Job{
				Task: Task{
					TaskHeader: TaskHeader{
						BatchID: "1",
						ID:      "1",
					},
				},
				JobDesc: JobDesc{
					JobType: JobMap,
					Input:   input,
				},
				Status: StatusJobDelivery,
			},
		})

		rq.NoError(err)

		kvs, err := worker.store.RetrieveKV(output)
		rq.NoError(err)
		rq.Equal(1, len(kvs))
	})

	t.Run("reducef", func(t *testing.T) {
		const input = "work-reduce-1.txt"

		worker.reducef = func(string, []string) string {
			return "1"
		}
		defer func() {
			worker.reducef = mockReduceFn
		}()

		output, err := worker.handleReduceJob(&RPCParam{
			InstanceID: 100,
			Job: &Job{
				Task: Task{
					TaskHeader: TaskHeader{
						BatchID: "1",
						ID:      "1",
					},
				},
				JobDesc: JobDesc{
					JobType: JobReduce,
					Input:   input,
				},
				Status: StatusJobDelivery,
			},
		})

		rq.NoError(err)

		kvs, err := worker.store.RetrieveKV(output)
		rq.NoError(err)
		rq.Equal(1, len(kvs))
	})
}

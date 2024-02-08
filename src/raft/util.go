package raft

import (
	"errors"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var (
	errorWorkerStopped = errors.New("error worker stopped")

	errorLogIndexOutOfRange = errors.New("log index out of range")
	errorIllegalLogIndex    = errors.New("log index should not less than zero")
)

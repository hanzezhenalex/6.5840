package raft

import (
	"errors"
	"log"
)

func DPrintf(format string, a ...interface{}) {
	log.Printf(format, a...)
}

var (
	errorWorkerStopped = errors.New("error worker stopped")

	errorLogIndexOutOfRange = errors.New("log index out of range")
	errorIllegalLogIndex    = errors.New("log index should not less than 1")

	errorTimeout       = errors.New("timeout")
	errorSendReqToPeer = errors.New("fail to send RPC request to peer")
)

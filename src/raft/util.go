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

	errorTimeout       = errors.New("timeout")
	errorSendReqToPeer = errors.New("fail to send RPC request to peer")
)

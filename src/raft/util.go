package raft

import (
	"fmt"
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

func AssertTrue(cond bool, format string, msg ...any) {
	if !cond {
		panic(fmt.Sprintf(format, msg...))
	}
}

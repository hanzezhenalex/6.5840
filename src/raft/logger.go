package raft

import (
	"fmt"
	"go.uber.org/zap"
	"os"
)

func GetBaseLogger() (*zap.Logger, error) {
	if os.Getenv("MR_PROD") == "true" {
		cfg := zap.NewProductionConfig()

		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
		cfg.DisableStacktrace = true
		cfg.DisableCaller = true
		cfg.Development = false

		return cfg.Build()
	}
	return zap.NewDevelopment()
}

func GetLogger(component string) (*zap.Logger, error) {
	base, err := GetBaseLogger()
	if err != nil {
		return nil, fmt.Errorf("fail to get base logger, %w", err)
	}
	return base.With(zap.String(LoggerComponent, component)), nil
}

func GetLoggerOrPanic(component string) *zap.Logger {
	logger, err := GetLogger(component)
	if err != nil {
		panic(err)
	}
	return logger
}

const (
	LoggerComponent = "component"
	Term            = "term"
	Peer            = "peer"
	Index           = "index"
	PeerTerm        = "peer term"
)

const (
	followerTimeout       = "follower timeout"
	candidateTimeout      = "candidate timeout"
	candidateBecomeLeader = "candidate become leader"
)

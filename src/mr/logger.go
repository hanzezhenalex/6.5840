package mr

import (
	"fmt"

	"go.uber.org/zap"
)

func GetBaseLogger() (*zap.Logger, error) {
	return zap.NewDevelopment()
}

func GetLogger(component string) (*zap.Logger, error) {
	base, err := zap.NewDevelopment()
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

const LoggerComponent = "component"

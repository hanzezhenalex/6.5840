package mr

import (
	"fmt"

	"go.uber.org/zap"
)

var production = false

func SetProductionMode() {
	production = true
}

func GetBaseLogger() (*zap.Logger, error) {
	if production {
		return zap.NewProduction()
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

const LoggerComponent = "component"

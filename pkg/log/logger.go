package log

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"sync"
)

const (
	DebugLevel = "debug"
	InfoLevel  = "info"
	WarnLevel  = "warn"
	ErrorLevel = "error"

	defaultLogLevel       = WarnLevel
	defaultRootLoggerName = "v3io-tsdb"
)

var once sync.Once
var instance logger.Logger

func GetOrCreateRootLogger(logLevel string) (logger.Logger, error) {
	var err error
	once.Do(func() {
		var errorMsg string
		if logLevel != DebugLevel && logLevel != InfoLevel && logLevel != WarnLevel && logLevel != ErrorLevel {
			errorMsg = fmt.Sprintf("invalid log level has been received (input = '%s'), will use default log level: `%s`", logLevel, defaultLogLevel)
			logLevel = defaultLogLevel
		}
		if logLevel == "" {
			logLevel = defaultLogLevel
		}
		instance, err = newZapLogger(defaultRootLoggerName, logLevel)
		if errorMsg != "" {
			instance.Warn(errorMsg)
		}
	})

	return instance, err
}

func Logger(childLoggerName string) logger.Logger {
	if instance == nil {
		GetOrCreateRootLogger(defaultLogLevel)
	}
	return instance.GetChild(childLoggerName)
}

func newZapLogger(rootLoggerName, level string) (logger.Logger, error) {
	if rootLoggerName == "" {
		rootLoggerName = "v3io-prom"
	}
	var logLevel nucliozap.Level
	switch level {
	case DebugLevel:
		logLevel = nucliozap.DebugLevel
	case InfoLevel:
		logLevel = nucliozap.InfoLevel
	case WarnLevel:
		logLevel = nucliozap.WarnLevel
	case ErrorLevel:
		logLevel = nucliozap.ErrorLevel
	default:
		logLevel = nucliozap.WarnLevel
	}

	log, err := nucliozap.NewNuclioZapCmd(rootLoggerName, logLevel)
	if err != nil {
		return nil, err
	}
	return log, nil
}

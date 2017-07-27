package log

import (
	"os"

	"github.com/op/go-logging"
	"github.com/upfluence/goutils/error_logger"
)

const (
	defaultStdoutLevel      = logging.NOTICE
	defaultErrorLoggerLevel = logging.ERROR
)

var (
	logger = &logging.Logger{Module: "upfluence", ExtraCalldepth: 1}
	format = logging.MustStringFormatter(
		`[%{level:.1s} %{time:060102 15:04:05} %{shortfile}] %{message}`,
	)

	stdoutBackend = logging.AddModuleLevel(
		logging.NewBackendFormatter(
			logging.NewLogBackend(os.Stdout, "", 0),
			format,
		),
	)

	errBackend = logging.AddModuleLevel(
		&errorLoggerBackend{client: error_logger.DefaultErrorLogger},
	)
)

func init() {
	var (
		level logging.Level
		err   error
	)

	if level, err = logging.LogLevel(os.Getenv("LOGGER_LEVEL")); err != nil {
		level = defaultStdoutLevel
	}

	stdoutBackend.SetLevel(level, "")
	errBackend.SetLevel(defaultErrorLoggerLevel, "")

	logging.SetBackend(stdoutBackend, errBackend)
}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func Panic(args ...interface{}) {
	logger.Panic(args...)
}

func Panicf(format string, args ...interface{}) {
	logger.Panicf(format, args...)
}

func Critical(args ...interface{}) {
	logger.Critical(args...)
}

func Criticalf(format string, args ...interface{}) {
	logger.Criticalf(format, args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func Warning(args ...interface{}) {
	logger.Warning(args...)
}

func Warningf(format string, args ...interface{}) {
	logger.Warningf(format, args...)
}

func Notice(args ...interface{}) {
	logger.Notice(args...)
}

func Noticef(format string, args ...interface{}) {
	logger.Noticef(format, args...)
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

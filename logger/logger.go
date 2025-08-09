package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger struct {
	*zap.Logger
}

var (
	once   sync.Once
	logger *Logger
)

type Config struct {
	LogFile   string
	LogLevel  string
	AppName   string
	AddCaller bool
}

func Init(cfg Config) error {
	var err error
	once.Do(func() {
		logger, err = newLogger(cfg)
	})
	return err
}

func newLogger(cfg Config) (*Logger, error) {
	logLevel := parseLogLevel(cfg.LogLevel)

	jsonEncoder := zapcore.NewJSONEncoder(makeProductionEncoderConfig())
	consoleEncoder := zapcore.NewConsoleEncoder(makeDevelopmentEncoderConfig())

	cores := []zapcore.Core{}

	stdoutCore := zapcore.NewCore(
		consoleEncoder,
		zapcore.Lock(os.Stdout),
		logLevel,
	)
	cores = append(cores, stdoutCore)

	if cfg.LogFile != "" {
		logFile := &lumberjack.Logger{
			Filename:   cfg.LogFile,
			MaxSize:    500, // megabytes
			MaxBackups: 3,
			MaxAge:     28,   //days
			Compress:   true, // disabled by default
		}

		fileCore := zapcore.NewCore(
			jsonEncoder,
			zapcore.AddSync(logFile),
			logLevel,
		)
		cores = append(cores, fileCore)
	}

	core := zapcore.NewTee(cores...)

	opts := []zap.Option{
		zap.Fields(zap.String("service", cfg.AppName)),
	}
	if cfg.AddCaller {
		opts = append(opts, zap.AddCaller(), zap.AddCallerSkip(1))
	}

	return &Logger{
		zap.New(core, opts...),
	}, nil
}

func parseLogLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func makeProductionEncoderConfig() zapcore.EncoderConfig {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.TimeKey = "timestamp"
	cfg.LevelKey = "severity"
	return cfg
}

func makeDevelopmentEncoderConfig() zapcore.EncoderConfig {
	cfg := zap.NewDevelopmentEncoderConfig()
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.TimeKey = "time"
	cfg.LevelKey = "level"
	cfg.MessageKey = "message"
	cfg.CallerKey = "caller"
	return cfg
}

func Sync() error {
	if logger != nil {
		return logger.Sync()
	}
	return nil
}

func Get() *Logger {
	if logger == nil {
		fallbackLogger, _ := zap.NewDevelopment()
		return &Logger{fallbackLogger}
	}
	return logger
}

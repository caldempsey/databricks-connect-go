package databricks

import (
	"fmt"
	"log"
	"log/slog"
)

// StdLogger wraps the standard library logger
type StdLogger struct {
	logger *log.Logger
}

// NewStdLogger creates a Logger using the standard library logger
func NewStdLogger(l *log.Logger) Logger {
	if l == nil {
		l = log.Default()
	}
	return &StdLogger{logger: l}
}

func (s *StdLogger) Debug(msg string, keysAndValues ...any) {
	s.logger.Printf("[DEBUG] %s %v", msg, keysAndValues)
}

func (s *StdLogger) Info(msg string, keysAndValues ...any) {
	s.logger.Printf("[INFO] %s %v", msg, keysAndValues)
}

func (s *StdLogger) Warn(msg string, keysAndValues ...any) {
	s.logger.Printf("[WARN] %s %v", msg, keysAndValues)
}

func (s *StdLogger) Error(msg string, keysAndValues ...any) {
	s.logger.Printf("[ERROR] %s %v", msg, keysAndValues)
}

// SlogAdapter wraps slog.Logger
type SlogAdapter struct {
	logger *slog.Logger
}

// NewSlogAdapter creates a Logger using slog
func NewSlogAdapter(l *slog.Logger) Logger {
	if l == nil {
		l = slog.Default()
	}
	return &SlogAdapter{logger: l}
}

func (s *SlogAdapter) Debug(msg string, keysAndValues ...any) {
	s.logger.Debug(msg, keysAndValues...)
}

func (s *SlogAdapter) Info(msg string, keysAndValues ...any) {
	s.logger.Info(msg, keysAndValues...)
}

func (s *SlogAdapter) Warn(msg string, keysAndValues ...any) {
	s.logger.Warn(msg, keysAndValues...)
}

func (s *SlogAdapter) Error(msg string, keysAndValues ...any) {
	s.logger.Error(msg, keysAndValues...)
}

// SimpleLogger is a basic implementation that prints to stdout
type SimpleLogger struct {
	level LogLevel
}

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// NewSimpleLogger creates a simple logger with the specified level
func NewSimpleLogger(level LogLevel) Logger {
	return &SimpleLogger{level: level}
}

func (s *SimpleLogger) Debug(msg string, keysAndValues ...any) {
	if s.level <= DebugLevel {
		s.log("DEBUG", msg, keysAndValues...)
	}
}

func (s *SimpleLogger) Info(msg string, keysAndValues ...any) {
	if s.level <= InfoLevel {
		s.log("INFO", msg, keysAndValues...)
	}
}

func (s *SimpleLogger) Warn(msg string, keysAndValues ...any) {
	if s.level <= WarnLevel {
		s.log("WARN", msg, keysAndValues...)
	}
}

func (s *SimpleLogger) Error(msg string, keysAndValues ...any) {
	if s.level <= ErrorLevel {
		s.log("ERROR", msg, keysAndValues...)
	}
}

func (s *SimpleLogger) log(level, msg string, keysAndValues ...any) {
	fmt.Printf("[%s] %s", level, msg)
	if len(keysAndValues) > 0 {
		fmt.Printf(" %v", keysAndValues)
	}
	fmt.Println()
}

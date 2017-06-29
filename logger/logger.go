// Copyright 2017 Apcera Inc. All rights reserved.

package logger

import (
	"sync"

	natsd "github.com/nats-io/gnatsd/server"
)

// LogPrefix is prefixed to all NATS Streaming log messages
const LogPrefix = "STREAM: "

// Logger interface for the Streaming project.
// This is an alias of the NATS Server's Logger interface.
type Logger natsd.Logger

// StanLogger is the logger used in this project and implements
// the Logger interface.
type StanLogger struct {
	mu    sync.RWMutex
	debug bool
	trace bool
	log   natsd.Logger
}

// NewStanLogger returns an instance of StanLogger
func NewStanLogger() *StanLogger {
	return &StanLogger{}
}

// SetLogger sets the logger, debug and trace
func (s *StanLogger) SetLogger(log Logger, debug, trace bool) {
	s.mu.Lock()
	s.log = log
	s.debug = debug
	s.trace = trace
	s.mu.Unlock()
}

// GetLogger returns the logger
func (s *StanLogger) GetLogger() Logger {
	s.mu.RLock()
	l := s.log
	s.mu.RUnlock()
	return l
}

// Noticef logs a notice statement
func (s *StanLogger) Noticef(format string, v ...interface{}) {
	s.executeLogCall(func(log Logger, format string, v ...interface{}) {
		log.Noticef(format, v...)
	}, format, v...)
}

// Errorf logs an error
func (s *StanLogger) Errorf(format string, v ...interface{}) {
	s.executeLogCall(func(log Logger, format string, v ...interface{}) {
		log.Errorf(format, v...)
	}, format, v...)
}

// Fatalf logs a fatal error
func (s *StanLogger) Fatalf(format string, v ...interface{}) {
	s.executeLogCall(func(log Logger, format string, v ...interface{}) {
		log.Fatalf(format, v...)
	}, format, v...)
}

// Debugf logs a debug statement
func (s *StanLogger) Debugf(format string, v ...interface{}) {
	s.executeLogCall(func(log Logger, format string, v ...interface{}) {
		// This is running under the protection of StanLogging's lock
		if s.debug {
			log.Debugf(format, v...)
		}
	}, format, v...)
}

// Tracef logs a trace statement
func (s *StanLogger) Tracef(format string, v ...interface{}) {
	s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
		if s.trace {
			logger.Tracef(format, v...)
		}
	}, format, v...)
}

func (s *StanLogger) executeLogCall(f func(logger Logger, format string, v ...interface{}), format string, args ...interface{}) {
	s.mu.Lock()
	if s.log == nil {
		s.mu.Unlock()
		return
	}
	f(s.log, LogPrefix+format, args...)
	s.mu.Unlock()
}

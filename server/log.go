// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"fmt"

	"github.com/nats-io/gnatsd/server"
)

var noLog bool

// Noticef logs a notice statement
func Noticef(format string, v ...interface{}) {
	server.Noticef(format, v...)
}

// Errorf logs an error statement
func Errorf(format string, v ...interface{}) {
	if noLog {
		fmt.Printf(format, v...)
	} else {
		server.Errorf(format, v...)
	}
}

// Fatalf logs a fatal error
func Fatalf(format string, v ...interface{}) {
	server.Fatalf(format, v...)
}

// Debugf logs a debug statement
func Debugf(format string, v ...interface{}) {
	server.Debugf(format, v...)
}

// Tracef logs a trace statement
func Tracef(format string, v ...interface{}) {
	server.Tracef(format, v...)
}

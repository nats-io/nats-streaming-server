// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"fmt"

	"github.com/nats-io/gnatsd/server"
)

var noLog bool

func Noticef(format string, v ...interface{}) {
	server.Noticef(format, v...)
}
func Errorf(format string, v ...interface{}) {
	if noLog {
		fmt.Printf(format, v...)
	} else {
		server.Errorf(format, v...)
	}
}

func Fatalf(format string, v ...interface{}) {
	server.Fatalf(format, v...)
}

func Debugf(format string, v ...interface{}) {
	server.Debugf(format, v...)
}

func Tracef(format string, v ...interface{}) {
	server.Tracef(format, v...)
}

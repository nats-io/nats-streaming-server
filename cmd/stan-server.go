// Copyright 2016 Apcera Inc. All rights reserved.
// +build ignore

package main

import (
	"flag"
	"os"
	"runtime"

	"github.com/nats-io/gnatsd/logger"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/stan"
)

func main() {

	var ID string

	// Parse flags
	flag.StringVar(&ID, "id", "test-cluster", "Cluster ID.")

	flag.Parse()

	opts := &server.Options{}
	configureLogger(opts)

	server.Noticef("Starting stan-server[%s] version %s", ID, stan.Version)

	stan.RunServer(ID, opts)

	runtime.Goexit()
}

func configureLogger(opts *server.Options) {
	//	var log natsd.Logger
	colors := true
	// Check to see if stderr is being redirected and if so turn off color
	// Also turn off colors if we're running on Windows where os.Stderr.Stat() returns an invalid handle-error
	stat, err := os.Stderr.Stat()
	if err != nil || (stat.Mode()&os.ModeCharDevice) == 0 {
		colors = false
	}
	log := logger.NewStdLogger(opts.Logtime, opts.Debug, opts.Trace, colors, true)

	var s *server.Server
	s.SetLogger(log, opts.Debug, opts.Trace)
}

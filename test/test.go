// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	gnatsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats-streaming-server/server"
)

// RunServer launches a server with the specified ID and default options.
func RunServer(ID string) *server.StanServer {
	s, err := server.RunServer(ID)
	if err != nil {
		panic(err)
	}
	return s
}

// RunServerWithOpts launches a server with the specified options.
func RunServerWithOpts(stanOpts *server.Options, natsOpts *gnatsd.Options) *server.StanServer {
	s, err := server.RunServerWithOpts(stanOpts, natsOpts)
	if err != nil {
		panic(err)
	}
	return s
}

// RunServerWithDebugTrace is a helper to assist debugging
func RunServerWithDebugTrace(ID string, enableSTANDebug, enableSTANTrace, enableNATSDebug, enableNATSTrace bool) *server.StanServer {

	nOpts := server.DefaultNatsServerOptions
	nOpts.Debug = enableNATSDebug
	nOpts.Trace = enableNATSTrace
	nOpts.NoLog = false

	sOpts := server.GetDefaultOptions()
	sOpts.Debug = enableSTANDebug
	sOpts.Trace = enableSTANTrace
	sOpts.ID = ID

	// enable logging
	server.ConfigureLogger(sOpts, &nOpts)

	s, err := server.RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		panic(err)
	}
	return s
}

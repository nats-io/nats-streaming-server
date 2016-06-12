// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"github.com/nats-io/nats-streaming-server/server"
)

// RunServer launches a server with the specified ID and default options.
func RunServer(ID string) *server.StanServer {
	return server.RunServer(ID)
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

	return server.RunServerWithOpts(sOpts, &nOpts)
}

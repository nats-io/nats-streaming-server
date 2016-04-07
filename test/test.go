// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	natsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/stan-server/server"
)

// RunServer launches a server with the specified ID and default options.
func RunServer(ID string) *server.StanServer {
	return server.RunServer(ID)
}

// RunServerWithDebugTrace is a helper to assist debugging
func RunServerWithDebugTrace(ID string, enableDebug, enableTrace bool) *server.StanServer {
	natsdOpts := &natsd.Options{}

	natsdOpts.Debug = enableDebug
	natsdOpts.Trace = enableTrace
	natsdOpts.NoLog = false

	stanOpts := &server.DefaultOptions
	stanOpts.ID = ID

	server.EnableDefaultLogger(natsdOpts)

	return server.RunServerWithOpts(stanOpts, natsdOpts)
}

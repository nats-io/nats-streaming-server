// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"github.com/nats-io/stan-server/server"
)

// RunServer will startup and embedded STAN server and a nats-server to support it.
func RunServer(ID string) *server.StanServer {
	return server.RunServer(ID, "")
}

// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"github.com/nats-io/stanserver/server"
)

func RunServer(ID string) *server.StanServer {
	return server.RunServer(ID)
}

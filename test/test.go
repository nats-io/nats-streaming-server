// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"github.com/nats-io/stan-server/server"
)

func RunServer(ID string) *server.StanServer {
	s, _ := server.RunServer(ID, "")
	return s
}

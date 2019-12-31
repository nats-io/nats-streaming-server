// Copyright 2017-2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !windows

package server

import (
	"os"
	"os/signal"
	"syscall"

	natsd "github.com/nats-io/nats-server/v2/server"
)

func init() {
	// Set the process name so signal code use this process name
	// instead of gnatsd.
	natsd.SetProcessName("nats-streaming-server")
}

// Signal Handling
func (s *StanServer) handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGHUP)
	go func() {
		for {
			select {
			case sig := <-c:
				// Notify will relay only the signals that we have
				// registered, so we don't need a "default" in the
				// switch statement.
				switch sig {
				case syscall.SIGINT:
					s.Shutdown()
					os.Exit(0)
				case syscall.SIGTERM:
					s.Shutdown()
					os.Exit(143)
				case syscall.SIGUSR1:
					// File log re-open for rotating file logs.
					s.log.ReopenLogFile()
				case syscall.SIGHUP:
					// Ignore for now
				}
			case <-s.shutdownCh:
				return
			}
		}
	}()
}

// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"os"
	"os/signal"
)

// Signal Handling
func (s *StanServer) handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		// We register only 1 signal (os.Interrupt) so we don't
		// need to check which one we get, since Notify() relays
		// only the ones that are registered.
		<-c
		s.Shutdown()
		os.Exit(0)
	}()
}

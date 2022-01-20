// Copyright 2022 The NATS Authors
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

//go:build !windows
// +build !windows

package server

import (
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	sOpts := GetDefaultOptions()
	srvCh := make(chan *StanServer, 1)
	errCh := make(chan error, 1)
	go func() {
		s, err := Run(sOpts, nil)
		srvCh <- s
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Failed to start")
	}

	s := <-srvCh
	if s == nil {
		t.Fatalf("Server is nil")
	}
	defer s.Shutdown()
	if s.State() != Standalone {
		t.Fatalf("Unexpected sate: %v", s.State().String())
	}
}

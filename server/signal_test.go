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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	natsd "github.com/nats-io/nats-server/v2/server"
	natsdTest "github.com/nats-io/nats-server/v2/test"
)

func TestSignalIgnoreUnknown(t *testing.T) {
	opts := GetDefaultOptions()
	opts.HandleSignals = true
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	// Send signal that we don't handle
	syscall.Kill(syscall.Getpid(), syscall.SIGUSR2)
	// Check server is still active
	time.Sleep(250 * time.Millisecond)
	if state := s.State(); state != Standalone {
		t.Fatalf("Expected state to be %v, got %v", Standalone.String(), state.String())
	}
}

func TestSignalToReOpenLogFile(t *testing.T) {
	logFile := "test.log"
	f := func(iter int) {
		defer os.Remove(logFile)
		defer os.Remove(logFile + ".bak")

		var ns *natsd.Server
		if iter == 0 {
			ns = natsdTest.RunDefaultServer()
			defer ns.Shutdown()
		}
		sopts := GetDefaultOptions()
		sopts.HandleSignals = true
		nopts := &natsd.Options{
			Host:    "localhost",
			Port:    -1,
			NoSigs:  true,
			LogFile: logFile,
		}
		sopts.EnableLogging = true
		if iter == 0 {
			sopts.NATSServerURL = "nats://" + ns.Addr().String()
		}
		s := runServerWithOpts(t, sopts, nopts)
		defer s.Shutdown()

		// Repeat twice to ensure that signal is processed more than once
		for i := 0; i < 2; i++ {
			// Add a trace
			expectedStr := "This is a Notice"
			s.log.Noticef(expectedStr)
			buf, err := ioutil.ReadFile(logFile)
			if err != nil {
				t.Fatalf("Error reading file: %v", err)
			}
			if !strings.Contains(string(buf), expectedStr) {
				t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
			}
			// Rename the file
			if err := os.Rename(logFile, logFile+".bak"); err != nil {
				t.Fatalf("Unable to rename file: %v", err)
			}
			// This should cause file to be reopened.
			syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
			// Wait a bit for action to be performed
			waitFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				buf, err = ioutil.ReadFile(logFile)
				if err != nil {
					return fmt.Errorf("Error reading file: %v", err)
				}
				expectedStr = "File log re-opened"
				if !strings.Contains(string(buf), expectedStr) {
					return fmt.Errorf("Expected log to contain %q, got %q", expectedStr, string(buf))
				}
				return nil
			})
			// Make sure that new traces are added
			expectedStr = "This is a new notice"
			s.log.Noticef(expectedStr)
			buf, err = ioutil.ReadFile(logFile)
			if err != nil {
				t.Fatalf("Error reading file: %v", err)
			}
			if !strings.Contains(string(buf), expectedStr) {
				t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
			}
		}
	}
	for iter := 0; iter < 2; iter++ {
		f(iter)
	}
}

type stderrCatcher struct {
	sync.Mutex
	b []byte
}

func (sc *stderrCatcher) Write(p []byte) (n int, err error) {
	sc.Lock()
	sc.b = append(sc.b, p...)
	sc.Unlock()
	return len(p), nil
}

func TestSignalTrapsSIGTERM(t *testing.T) {
	// This test requires that the
	cmd := exec.Command("nats-streaming-server")
	sc := &stderrCatcher{}
	cmd.Stderr = sc
	cmd.Start()
	// Wait for it to print some startup trace
	waitFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sc.Lock()
		ready := bytes.Contains(sc.b, []byte("STREAM: ------------------"))
		sc.Unlock()
		if ready {
			return nil
		}
		return fmt.Errorf("process not started yet")
	})
	syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
	cmd.Wait()
	sc.Lock()
	gotIt := bytes.Contains(sc.b, []byte("Shutting down"))
	sc.Unlock()
	if !gotIt {
		t.Fatalf("Did not get the Shutting down trace (make sure you did a `go install` prior to running the test")
	}
}

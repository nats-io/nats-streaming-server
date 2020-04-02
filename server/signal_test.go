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
			Host:    "127.0.0.1",
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
	// This test requires that the server be installed.
	cmd := exec.Command("nats-streaming-server")
	sc := &stderrCatcher{}
	cmd.Stderr = sc
	cmd.Start()
	// Wait for it to print some startup trace
	waitFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sc.Lock()
		ready := bytes.Contains(sc.b, []byte(streamingReadyLog))
		sc.Unlock()
		if ready {
			return nil
		}
		return fmt.Errorf("process not started yet, make sure you `go install` first!")
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

func createConfFile(t *testing.T, content []byte) string {
	t.Helper()
	conf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	fName := conf.Name()
	conf.Close()
	if err := ioutil.WriteFile(fName, content, 0666); err != nil {
		os.Remove(fName)
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}

func changeCurrentConfigContentWithNewContent(t *testing.T, curConfig string, content []byte) {
	t.Helper()
	if err := ioutil.WriteFile(curConfig, content, 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
}

func TestSignalReload(t *testing.T) {
	conf := createConfFile(t, []byte(`debug: false`))
	defer os.Remove(conf)
	// This test requires that the server be installed.
	cmd := exec.Command("nats-streaming-server", "-c", conf)
	sc := &stderrCatcher{}
	cmd.Stderr = sc
	cmd.Start()
	// Wait for it to print some startup trace
	waitFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sc.Lock()
		ready := bytes.Contains(sc.b, []byte(streamingReadyLog))
		sc.Unlock()
		if ready {
			return nil
		}
		return fmt.Errorf("process not started yet, make sure you `go install` first!")
	})
	changeCurrentConfigContentWithNewContent(t, conf, []byte(`debug: true`))
	syscall.Kill(cmd.Process.Pid, syscall.SIGHUP)
	time.Sleep(500 * time.Millisecond)
	syscall.Kill(cmd.Process.Pid, syscall.SIGINT)
	cmd.Wait()
	sc.Lock()
	gotIt := bytes.Contains(sc.b, []byte("Reloaded: debug = true"))
	sc.Unlock()
	if !gotIt {
		t.Fatalf("Did not get the Reload trace (make sure you did a `go install` prior to running the test")
	}
}

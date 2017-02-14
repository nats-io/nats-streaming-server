// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	natsd "github.com/nats-io/gnatsd/server"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestConfigureLogger(t *testing.T) {

	defer RemoveLogger()
	defer setDebugAndTraceToDefaultOptions(true)

	setDebugAndTraceToDefaultOptions(false)

	checkDebugTraceOff := func() {
		if debug != 0 || trace != 0 {
			t.Fatalf("Expected debug/trace to be disabled")
		}
	}

	// Test nil options
	ConfigureLogger(nil, nil)
	checkDebugTraceOff()

	sOpts := GetDefaultOptions()
	nOpts := natsd.Options{}

	// Neither enabled (defaults are off)
	ConfigureLogger(sOpts, &nOpts)
	checkDebugTraceOff()

	// NATS debug options enabled, stan off
	nOpts.Debug = true
	nOpts.Trace = true
	ConfigureLogger(sOpts, &nOpts)
	checkDebugTraceOff()

	// STAN debug options enabled, nats off
	nOpts.Debug = false
	nOpts.Trace = false
	sOpts.Debug = true
	sOpts.Trace = true
	ConfigureLogger(sOpts, &nOpts)
	if debug == 0 || trace == 0 {
		t.Fatalf("Expected debug/trace to be enabled")
	}

	// All enabled... (coverage)
	nOpts.Debug = true
	nOpts.Trace = true
	sOpts.Debug = true
	sOpts.Trace = true
	ConfigureLogger(sOpts, &nOpts)
	if debug == 0 || trace == 0 {
		t.Fatalf("Expected debug/trace to be enabled")
	}

	// turn off logging we've enabled
	RemoveLogger()
}

func TestLogging(t *testing.T) {

	defer RemoveLogger()
	defer setDebugAndTraceToDefaultOptions(true)

	setDebugAndTraceToDefaultOptions(false)

	// test without a logger
	Noticef("noop")

	sOpts := GetDefaultOptions()
	nOpts := &natsd.Options{}

	// test stdout
	sOpts.Debug = true
	sOpts.Trace = true
	ConfigureLogger(sOpts, nOpts)

	// skip syslog until there is support in Travis
	//nOpts = &natsd.Options{}
	//nOpts.Syslog = true
	//ConfigureLogger(sOpts, nOpts)

	//nOpts = &natsd.Options{}
	//nOpts.RemoteSyslog = "udp://localhost:514"
	//ConfigureLogger(sOpts, nOpts)

	// test file
	tmpDir, err := ioutil.TempDir("", "_stan_server")
	if err != nil {
		t.Fatalf("Could not create tmp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	file, err := ioutil.TempFile(tmpDir, "stan_server:log_")
	if err != nil {
		t.Fatalf("Could not create tmp file: %v", err)
	}

	nOpts = &natsd.Options{}
	nOpts.LogFile = file.Name()
	ConfigureLogger(sOpts, nOpts)
}

type dummyLogger struct {
	msg string
}

func (d *dummyLogger) Noticef(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Debugf(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Tracef(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Errorf(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Fatalf(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Reset() {
	d.msg = ""
}

func TestLogOutput(t *testing.T) {
	defer RemoveLogger()
	defer setDebugAndTraceToDefaultOptions(true)

	setDebugAndTraceToDefaultOptions(false)

	// dummy to override the configured logger.
	d := &dummyLogger{}

	checkLogger := func(output string) {
		if d.msg != output {
			t.Fatalf("Unexpected logger message: %v", d.msg)
		}
		d.Reset()
	}

	sOpts := GetDefaultOptions()
	ConfigureLogger(sOpts, nil)

	// override the default logger.
	stanLog.Lock()
	stanLog.logger = d
	stanLog.Unlock()

	// write to our logger and check values
	Noticef("foo")
	checkLogger("foo")

	Errorf("foo")
	checkLogger("foo")

	Fatalf("foo")
	checkLogger("foo")

	// debug is NOT set, value should be empty.
	Debugf("foo")
	checkLogger("")

	// trace is NOT set, value should be empty.
	Tracef("foo")
	checkLogger("")

	// enable debug and trace
	sOpts.Debug = true
	sOpts.Trace = true

	// reconfigure with debug/trace enabled
	ConfigureLogger(sOpts, nil)

	// override the default logger.
	stanLog.Lock()
	stanLog.logger = d
	stanLog.Unlock()

	// Debug is set so we should have the value
	Debugf("foo")
	checkLogger("foo")

	// Trace is set so we should have the value
	Tracef("foo")
	checkLogger("foo")
}

func TestRunServerFailureLogsCause(t *testing.T) {
	defer RemoveLogger()
	defer setDebugAndTraceToDefaultOptions(true)

	setDebugAndTraceToDefaultOptions(false)

	// dummy to override the configured logger.
	d := &dummyLogger{}

	sOpts := GetDefaultOptions()
	sOpts.NATSServerURL = "nats://localhost:4444"
	ConfigureLogger(sOpts, nil)

	// override the default logger.
	stanLog.Lock()
	stanLog.logger = d
	stanLog.Unlock()

	// We expect the server to fail to start
	var s *StanServer
	defer func() {
		if s != nil {
			t.Fatal("Expected no server to be returned")
		}
		if r := recover(); r != nil {
			// We should get a trace in the log_
			if !strings.Contains(d.msg, "Can't connect to NATS") {
				t.Fatalf("Expected to get a cause as invalid connection, got: %v", d.msg)
			}
		}
	}()
	s = RunServerWithOpts(sOpts, nil)
}

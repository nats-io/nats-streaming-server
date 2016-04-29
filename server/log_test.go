// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	natsd "github.com/nats-io/gnatsd/server"
	"testing"
)

func TestConfigureLogger(t *testing.T) {

	checkDebugTraceOff := func() {
		if debug != 0 || trace != 0 {
			t.Fatalf("Expected debug/trace to be disabled.")
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
		t.Fatalf("Expected debug/trace to be enabled.")
	}

	// All enabled... (coverage)
	nOpts.Debug = false
	nOpts.Trace = false
	sOpts.Debug = true
	sOpts.Trace = true
	ConfigureLogger(sOpts, &nOpts)
	if debug == 0 || trace == 0 {
		t.Fatalf("Expected debug/trace to be enabled.")
	}

	// turn off logging we've enabled
	RemoveLogger()
}

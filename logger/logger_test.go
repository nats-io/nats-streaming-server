// Copyright 2017 Apcera Inc. All rights reserved.

package logger

import (
	"fmt"
	"strings"
	"testing"
)

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

func TestLogger(t *testing.T) {
	logger := NewStanLogger()

	check := func(log Logger, debug, trace bool) {
		logger.mu.RLock()
		l, dbg, trc := logger.log, logger.debug, logger.trace
		logger.mu.RUnlock()
		if log != l {
			t.Fatalf("Expected log to be %v, got %v", log, l)
		}
		if debug != dbg {
			t.Fatalf("Expected debug to be %v, got %v", debug, dbg)
		}
		if trace != trc {
			t.Fatalf("Expected trace to be %v, got %v", trace, trc)
		}
	}
	check(nil, false, false)
	// This should not produce any logging, but should not crash
	logger.Noticef("Should not crash")

	logger.SetLogger(nil, false, false)
	check(nil, false, false)

	logger.SetLogger(nil, true, false)
	check(nil, true, false)

	logger.SetLogger(nil, false, true)
	check(nil, false, true)

	logger.SetLogger(nil, true, true)
	check(nil, true, true)

	dl := &dummyLogger{}
	logger.SetLogger(dl, false, false)
	check(dl, false, false)

	ls := logger.GetLogger()
	if dl != ls {
		t.Fatalf("Expected GetLogger to return %v, got %v", dl, ls)
	}

	checkLogger := func(output string) {
		prefixRemoved := strings.TrimPrefix(dl.msg, LogPrefix)
		if output != prefixRemoved {
			t.Fatalf("Unexpected logger message: \"%v\" != \"%v\"", prefixRemoved, output)
		}
		dl.Reset()
	}

	// write to our logger and check values
	logger.Noticef("foo")
	checkLogger("foo")

	logger.Errorf("foo")
	checkLogger("foo")

	logger.Fatalf("foo")
	checkLogger("foo")

	// debug is NOT set, value should be empty.
	logger.Debugf("foo")
	checkLogger("")

	// trace is NOT set, value should be empty.
	logger.Tracef("foo")
	checkLogger("")

	// enable debug and trace
	logger.SetLogger(dl, true, true)

	// Debug is set so we should have the value
	logger.Debugf("foo")
	checkLogger("foo")

	// Trace is set so we should have the value
	logger.Tracef("foo")
	checkLogger("foo")
}

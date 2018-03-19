// Copyright 2017-2018 The NATS Authors
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

package logger

import (
	"flag"
	"fmt"
	"strings"
	"testing"
)

func TestMain(_ *testing.M) {
	// This one is added here so that if we want to disable sql for stores tests
	// we can use the same param for all packages as in "go test -v ./... -sql=false"
	flag.Bool("sql", false, "Not used for logger tests")
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

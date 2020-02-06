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

package logger

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	natsdLogger "github.com/nats-io/nats-server/v2/logger"
)

func TestMain(m *testing.M) {
	// This one is added here so that if we want to disable sql for stores tests
	// we can use the same param for all packages as in "go test -v ./... -sql=false"
	flag.Bool("sql", false, "Not used for logger tests")
	os.Exit(m.Run())
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

func (d *dummyLogger) Warnf(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Close() error {
	return errors.New("dummy error")
}

func (d *dummyLogger) Reset() {
	d.msg = ""
}

func TestLogger(t *testing.T) {
	logger := NewStanLogger()

	check := func(log Logger, logtime, debug, trace bool, logfile string) {
		logger.mu.RLock()
		l, ltime, dbg, trc, lfile := logger.log, logger.ltime, logger.debug, logger.trace, logger.lfile
		logger.mu.RUnlock()
		if log != l {
			t.Fatalf("Expected log to be %v, got %v", log, l)
		}
		if logtime != ltime {
			t.Fatalf("Expected ltime to be %v, got %v", logtime, ltime)
		}
		if debug != dbg {
			t.Fatalf("Expected debug to be %v, got %v", debug, dbg)
		}
		if trace != trc {
			t.Fatalf("Expected trace to be %v, got %v", trace, trc)
		}
		if logfile != lfile {
			t.Fatalf("Expected lfile to be %v, got %v", logfile, lfile)
		}
	}
	check(nil, false, false, false, "")
	// This should not produce any logging, but should not crash
	logger.Noticef("Should not crash")

	logger.SetLogger(nil, false, false, false, "")
	check(nil, false, false, false, "")

	logger.SetLogger(nil, true, false, false, "")
	check(nil, true, false, false, "")

	logger.SetLogger(nil, false, true, false, "")
	check(nil, false, true, false, "")

	logger.SetLogger(nil, false, false, true, "")
	check(nil, false, false, true, "")

	logger.SetLogger(nil, false, false, false, "test.log")
	check(nil, false, false, false, "test.log")

	logger.SetLogger(nil, true, false, false, "test.log")
	check(nil, true, false, false, "test.log")

	logger.SetLogger(nil, true, true, false, "test.log")
	check(nil, true, true, false, "test.log")

	logger.SetLogger(nil, true, true, true, "test.log")
	check(nil, true, true, true, "test.log")

	dl := &dummyLogger{}
	logger.SetLogger(dl, false, false, false, "")
	check(dl, false, false, false, "")

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
	logger.SetLogger(dl, false, true, true, "")

	// Debug is set so we should have the value
	logger.Debugf("foo")
	checkLogger("foo")

	// Trace is set so we should have the value
	logger.Tracef("foo")
	checkLogger("foo")

	// Trying to re-open should log that this is not a filelog based
	logger.ReopenLogFile()
	checkLogger("File log re-open ignored, not a file logger")

	// Set a filename but still use the dummy logger
	logger.SetLogger(dl, true, true, true, "dummy.log")
	// Try to re-open, this should produce an error
	logger.ReopenLogFile()
	checkLogger("Unable to close logger: dummy error")

	// Switch to file log
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("Unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test.log")
	fl := natsdLogger.NewFileLogger(fname, true, true, true, true)
	logger.SetLogger(fl, true, true, true, fname)
	logger.SetFileSizeLimit(1000)
	// Reopen and check file content
	logger.ReopenLogFile()
	buf, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	expectedStr := "File log re-opened"
	if !strings.Contains(string(buf), expectedStr) {
		t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
	}
	// Make sure that the file size limit was applied to the new file
	notice := "This is some notice..."
	for i := 0; i < 100; i++ {
		logger.Noticef(notice)
	}
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Unable to read temp dir: %v", err)
	}
	if len(files) == 1 {
		t.Fatalf("Size limit was not applied")
	}
	logger.Close()
	if err := os.Remove(fname); err != nil {
		t.Fatalf("Unable to remove log file: %v", err)
	}
}

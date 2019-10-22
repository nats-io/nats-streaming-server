// Copyright 2016-2018 The NATS Authors
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

package util

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// This one is added here so that if we want to disable sql for stores tests
	// we can use the same param for all packages as in "go test -v ./... -sql=false"
	flag.Bool("sql", false, "Not used for util tests")
	os.Exit(m.Run())
}

func stackFatalf(t *testing.T, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
	// For staticcheck SA0511...
	panic("unreachable code")
}

func TestEnsureBufBigEnough(t *testing.T) {
	buf := make([]byte, 3)
	newBuf := EnsureBufBigEnough(buf, 2)
	if len(newBuf) != len(buf) {
		t.Fatal("EnsureBufBigEnough should not have allocated a new buffer")
	}
	newBuf = EnsureBufBigEnough(buf, 10)
	if len(newBuf) <= 10 {
		t.Fatalf("Buffer should be at least 10, it is: %v", len(newBuf))
	}
	newBuf = EnsureBufBigEnough(nil, 5)
	if len(newBuf) != 5 {
		t.Fatalf("Buffer should be exactly 5, it is: %v", len(newBuf))
	}
}

func TestWriteInt(t *testing.T) {
	fileName := "test.dat"
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer os.Remove(fileName)
	defer file.Close()
	if err := WriteInt(file, 123); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestReadInt(t *testing.T) {
	fileName := "test.dat"
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer os.Remove(fileName)
	defer file.Close()

	if _, err := ReadInt(file); err == nil {
		t.Fatal("Expected an error")
	}
	if err := WriteInt(file, 123); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if v, err := ReadInt(file); err != nil || v != 123 {
		t.Fatalf("Expected to read 123, got: %v (err=%v)", v, err)
	}
}

func TestCloseFile(t *testing.T) {
	fileName := "test.dat"
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer os.Remove(fileName)

	err = nil
	cferr := CloseFile(err, file)
	if cferr != nil {
		t.Fatalf("Unexpected error: %v", cferr)
	}

	err = fmt.Errorf("Previous error")
	cferr = CloseFile(err, file)
	if cferr != err {
		t.Fatalf("Expected original error to be untouched")
	}

	err = nil
	cferr = CloseFile(err, file)
	if cferr == err {
		t.Fatalf("Expected returned error to be different")
	}
}

func TestBackoffTimeCheck(t *testing.T) {
	// Check invalid values
	if btc, err := NewBackoffTimeCheck(-1, 1, time.Second); btc != nil || err == nil {
		t.Fatalf("NewBackoffTimeCheck returned: %v, %v", btc, err)
	}
	if btc, err := NewBackoffTimeCheck(0, 1, time.Second); btc != nil || err == nil {
		t.Fatalf("NewBackoffTimeCheck returned: %v, %v", btc, err)
	}
	if btc, err := NewBackoffTimeCheck(time.Second, 0, time.Second); btc != nil || err == nil {
		t.Fatalf("NewBackoffTimeCheck returned: %v, %v", btc, err)
	}
	if btc, err := NewBackoffTimeCheck(time.Second, -1, time.Second); btc != nil || err == nil {
		t.Fatalf("NewBackoffTimeCheck returned: %v, %v", btc, err)
	}
	if btc, err := NewBackoffTimeCheck(time.Second, 1, -1); btc != nil || err == nil {
		t.Fatalf("NewBackoffTimeCheck returned: %v, %v", btc, err)
	}
	if btc, err := NewBackoffTimeCheck(time.Second, 1, 0); btc != nil || err == nil {
		t.Fatalf("NewBackoffTimeCheck returned: %v, %v", btc, err)
	}
	if btc, err := NewBackoffTimeCheck(time.Second, 1, time.Millisecond); btc != nil || err == nil {
		t.Fatalf("NewBackoffTimeCheck returned: %v, %v", btc, err)
	}

	// Create a time check for printing.
	print, _ := NewBackoffTimeCheck(20*time.Millisecond, 2, 100*time.Millisecond)
	start := time.Now()
	if !print.Ok() {
		t.Fatal("Should have returned true")
	}
	if print.Ok() {
		if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
			t.Fatalf("Should have returned false, only %v elapsed", elapsed)
		}
	}
	start = time.Now()
	time.Sleep(30 * time.Millisecond)
	if !print.Ok() {
		if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
			t.Fatalf("Should have returned true, %v elapsed", elapsed)
		}
	}
	// Now Reset and call, it should succeed
	print.Reset()
	if !print.Ok() {
		t.Fatal("Should have returned true")
	}
	// Repeat calls until frequency is increased to the max
	freqs := make([]time.Duration, 0)
	last := time.Now()
	timeout := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(timeout) {
		if print.Ok() {
			freqs = append(freqs, time.Since(last))
			last = time.Now()
		}
	}
	// from the start, we should have printed, after the start at these times:
	// 0:00:20ms, 0:00:40ms, 0:00:80ms, 0:00:100ms, 0:00:200ms
	// but we max at 100, so expected values are:
	expected := []int64{20, 40, 80, 100, 100}
	if len(freqs) != 5 {
		t.Fatalf("Expected ok 5 times, got %v", len(freqs))
	}
	for i, f := range freqs {
		dur := time.Duration(expected[i] * int64(time.Millisecond))
		if f < dur-15*time.Millisecond || f > dur+15*time.Millisecond {
			t.Fatalf("Expected frequency to be +/- %v, got %v", dur, f)
		}
	}
	// Now that we know that we have reached the max frequency,
	// we are going to test the auto-reset. We need to wait that
	// 2x the max frequency pass *after* the allowed next print,
	// which at this point is 100ms ahead of us. So we need to
	// sleep for at least 300ms. Sleep a bit more.
	time.Sleep(500 * time.Millisecond)
	// At this point, it is as if we were calling for the first time:
	if !print.Ok() {
		t.Fatal("Should have returned true")
	}
	// Check internals
	if !print.nextTime.IsZero() {
		t.Fatal("No auto-reset done")
	}
}

func TestIsChannelNameValid(t *testing.T) {
	channel := ""
	for i := 0; i < 100; i++ {
		channel += "foo."
	}
	channel += "foo"
	if !IsChannelNameValid(channel, false) {
		t.Fatalf("Channel %q should be valid", channel)
	}
	channels := []string{
		"foo.bar*",
		"foo.bar>",
		"foo.bar.*",
		"foo.bar.>",
		"foo*.bar",
		"foo>.bar",
		"foo..bar",
		".foo.bar",
		"foo.bar.",
		"..",
		".",
		"foo/bar",
	}
	for _, s := range channels {
		if IsChannelNameValid(s, false) {
			t.Fatalf("Channel %q expected to be invalid", s)
		}
	}
	channels = []string{
		"foo.bar*",
		"foo.bar>",
		"foo*.bar",
		"foo>.bar",
		"foo.*bar",
		"foo.>bar",
		"foo.>.bar",
		">.",
		">.>",
		"foo..bar",
		".foo.bar",
		"foo.bar.",
		"..",
		".",
	}
	for _, s := range channels {
		if IsChannelNameValid(s, true) {
			t.Fatalf("Channel %q expected to be invalid", s)
		}
	}

	// Test valid wildcard channels
	channels = []string{
		"foo.*",
		"foo.*.*",
		"foo.>",
		"foo.*.>",
		"*",
		">",
		"*.bar.*",
		"*.bar.>",
		"*.>",
	}
	for _, s := range channels {
		if !IsChannelNameValid(s, true) {
			t.Fatalf("Channel %q expected to be valid", s)
		}
	}
}

func TestIsChannelNameLiteral(t *testing.T) {
	channels := []string{"foo.*", "foo.>", "foo.*.bar", "foo.bar.*"}
	for _, s := range channels {
		if IsChannelNameLiteral(s) {
			t.Fatalf("IsChannelNameLiteral for %q should have returned false", s)
		}
	}
	channels = []string{"foo.bar", "foo.baz", "foo.baz.bar", "foo.bar.baz"}
	for _, s := range channels {
		if !IsChannelNameLiteral(s) {
			t.Fatalf("IsChannelNameLiteral for %q should have returned true", s)
		}
	}
}

func TestFriendlyBytes(t *testing.T) {
	check := func(val int64, expectedSuffix string) {
		res := FriendlyBytes(val)
		if !strings.HasSuffix(res, expectedSuffix) {
			t.Fatalf("For %v, expected suffix to be %v, got %v", val, expectedSuffix, res)
		}
	}
	check(1000, " B")
	check(2000, " KB")
	check(10<<20, " MB")
	check(10<<30, " GB")
	check(10<<40, " TB")
	check(10<<50, " PB")
	check(0xFFFFFFFFFFFFFFF, " EB")
}

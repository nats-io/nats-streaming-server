// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"runtime"
	"strings"
)

// TLogger is used both in testing.B and testing.T so need to use a common interface
type TLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// StackFatalf produces a stack trace and passes it to t.Fatalf()
func StackFatalf(t TLogger, f string, args ...interface{}) {
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
}

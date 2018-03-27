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

package server

import (
	"io"
	"testing"
	"time"
)

func TestTimeoutReader(t *testing.T) {
	reader, writer := io.Pipe()
	r := newTimeoutReader(reader)

	r.SetDeadline(time.Now().Add(time.Millisecond))
	n, err := r.Read(make([]byte, 10))
	if err != ErrTimeout {
		t.Fatal("expected ErrTimeout")
	}
	if n != 0 {
		t.Fatalf("expected: 0\ngot: %d", n)
	}

	writer.Write([]byte("hello"))
	r.SetDeadline(time.Now().Add(time.Millisecond))
	buf := make([]byte, 5)
	n, err = r.Read(buf)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected: 5\ngot: %d", n)
	}
	if string(buf) != "hello" {
		t.Fatalf("expected: hello\ngot: %s", buf)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("error: %v", err)
	}

	n, err = r.Read(make([]byte, 5))
	if err != io.ErrClosedPipe {
		t.Fatalf("expected: ErrClosedPipe\ngot: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected: 0\ngot: %d", n)
	}
}

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

// +build windows

package util

import (
	"strings"
	"sync"
	"syscall"
)

type lockFile struct {
	sync.Mutex
	f syscall.Handle
}

// CreateLockFile attempt to lock the given file, creating it
// if necessary. On success, the file is returned, otherwise
// an error is returned.
// The file returned should be closed to release the lock
// quicker than if left to the operating system.
func CreateLockFile(file string) (LockFile, error) {
	fname, err := syscall.UTF16PtrFromString(file)
	if err != nil {
		return nil, err
	}
	f, err := syscall.CreateFile(fname,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0, // dwShareMode: 0 means "Prevents other processes from opening a file or device if they request delete, read, or write access."
		nil,
		syscall.CREATE_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		// TODO: There HAS to be a better way, but I can't seem to
		// find how to get Windows error codes (also syscall.GetLastError()
		// returns nil here).
		if strings.Contains(err.Error(), "used by another process") {
			err = ErrUnableToLockNow
		}
		syscall.CloseHandle(f)
		return nil, err
	}
	return &lockFile{f: f}, nil
}

// Close implements the LockFile interface
func (lf *lockFile) Close() error {
	lf.Lock()
	defer lf.Unlock()
	if lf.f == syscall.InvalidHandle {
		return nil
	}
	err := syscall.CloseHandle(lf.f)
	lf.f = syscall.InvalidHandle
	return err
}

// IsClosed implements the LockFile interface
func (lf *lockFile) IsClosed() bool {
	lf.Lock()
	defer lf.Unlock()
	return lf.f == syscall.InvalidHandle
}

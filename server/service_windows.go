// Copyright 2012-2019 The NATS Authors
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
	"fmt"
	"os"
	"strings"
	"sync"

	natsdLogger "github.com/nats-io/nats-server/v2/logger"
	natsd "github.com/nats-io/nats-server/v2/server"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/debug"
	"golang.org/x/sys/windows/svc/eventlog"
)

const (
	serviceName     = "nats-streaming-server"
	reopenLogCode   = 128
	reopenLogCmd    = svc.Cmd(reopenLogCode)
	acceptReopenLog = svc.Accepted(reopenLogCode)
)

// winServiceWrapper implements the svc.Handler interface for implementing
// nats-streaming-server as a Windows service.
type winServiceWrapper struct {
	sOpts *Options
	nOpts *natsd.Options
	srvCh chan *StanServer
	errCh chan error
}

var (
	dockerized     = false
	sysLogInitLock sync.Mutex
	sysLog         *eventlog.Log
	sysLogName     = "NATS-Streaming-Server"
)

func init() {
	if v, exists := os.LookupEnv("NATS_DOCKERIZED"); exists && v == "1" {
		dockerized = true
	}
	// Set the default event source name. This may be changed when the
	// server will configure the logger if a SyslogName option is specified.
	natsdLogger.SetSyslogName(sysLogName)
	// This is so the gnatsd's signal code works for streaming service
	natsd.SetServiceName(serviceName)
}

// Execute will be called by the package code at the start of
// the service, and the service will exit once Execute completes.
// Inside Execute you must read service change requests from r and
// act accordingly. You must keep service control manager up to date
// about state of your service by writing into s as required.
// args contains service name followed by argument strings passed
// to the service.
// You can provide service exit code in exitCode return parameter,
// with 0 being "no error". You can also indicate if exit code,
// if any, is service specific or not by using svcSpecificEC
// parameter.
func (w *winServiceWrapper) Execute(args []string, changes <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {

	status <- svc.Status{State: svc.StartPending}

	if sysLog != nil {
		sysLog.Info(1, "Starting NATS Streaming Server...")
	}
	// Override NoSigs since we are doing signal handling HERE
	w.sOpts.HandleSignals = false
	server, err := RunServerWithOpts(w.sOpts, w.nOpts)
	if err != nil && sysLog != nil {
		sysLog.Error(2, fmt.Sprintf("Starting server returned: %v", err))
	}
	if err != nil {
		w.errCh <- err
		// Failed to start.
		return true, 1
	}
	status <- svc.Status{
		State:   svc.Running,
		Accepts: svc.AcceptStop | svc.AcceptShutdown | svc.AcceptParamChange | acceptReopenLog,
	}
	w.srvCh <- server

loop:
	for change := range changes {
		switch change.Cmd {
		case svc.Interrogate:
			status <- change.CurrentStatus
		case svc.Stop, svc.Shutdown:
			status <- svc.Status{State: svc.StopPending}
			server.Shutdown()
			break loop
		case reopenLogCmd:
			// File log re-open for rotating file logs.
			server.log.ReopenLogFile()
		case svc.ParamChange:
		// Ignore for now
		default:
			server.log.Debugf("Unexpected control request: %v", change.Cmd)
		}
	}

	status <- svc.Status{State: svc.Stopped}
	return false, 0
}

// Run starts the NATS Streaming server. This wrapper function allows Windows to add a
// hook for running NATS Streaming as a service.
func Run(sOpts *Options, nOpts *natsd.Options) (*StanServer, error) {
	if dockerized {
		return RunServerWithOpts(sOpts, nOpts)
	}
	run := svc.Run
	isInteractive, err := svc.IsAnInteractiveSession()
	if err != nil {
		return nil, err
	}
	if isInteractive {
		run = debug.Run
	} else {
		sysLogInitLock.Lock()
		// We create a syslog here because we want to capture possible startup
		// failure message.
		if sysLog == nil {
			if sOpts.SyslogName != "" {
				sysLogName = sOpts.SyslogName
			}
			err := eventlog.InstallAsEventCreate(sysLogName, eventlog.Info|eventlog.Error|eventlog.Warning)
			if err != nil {
				if !strings.Contains(err.Error(), "registry key already exists") {
					panic(err)
				}
			}
			sysLog, err = eventlog.Open(sysLogName)
			if err != nil {
				panic(fmt.Sprintf("could not open event log: %v", err))
			}
		}
		sysLogInitLock.Unlock()
	}
	wrapper := &winServiceWrapper{
		srvCh: make(chan *StanServer, 1),
		errCh: make(chan error, 1),
		sOpts: sOpts,
		nOpts: nOpts,
	}
	go func() {
		// If no error, we exit here, otherwise, we are getting the
		// error down below.
		if err := run(serviceName, wrapper); err == nil {
			os.Exit(0)
		}
	}()

	var srv *StanServer
	// Wait for server instance to be created
	select {
	case err = <-wrapper.errCh:
	case srv = <-wrapper.srvCh:
	}
	return srv, err
}

// isWindowsService indicates if NATS is running as a Windows service.
func isWindowsService() bool {
	if dockerized {
		return false
	}
	isInteractive, _ := svc.IsAnInteractiveSession()
	return !isInteractive
}

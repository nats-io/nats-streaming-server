// Copyright 2016 Apcera Inc. All rights reserved.

package main

import (
	"flag"
	"os"
	"runtime"

	"fmt"
	natsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/stan"
	"github.com/nats-io/stan-server/server"
)

func main() {

	var ID string
	var filestore string

	opts := &natsd.Options{}

	// Parse flags
	parseFlags(opts, &ID, &filestore)

	server.EnableDefaultLogger(opts)

	server.Noticef("Starting stan-server[%s] version %s", ID, stan.Version)

	_, err := server.RunServer(ID, filestore, opts)
	if err != nil {
		os.Exit(1)
	}

	runtime.Goexit()
}

func parseFlags(opts *natsd.Options, ID, filestore *string) {

	var showVersion bool
	var debugAndTrace bool
	var showTLSHelp bool
	var configFile string

	// STAN options
	flag.StringVar(ID, "id", "test-cluster", "Cluster ID.")
	flag.StringVar(filestore, "filestore", "", "Root directory for file-based storage.")

	// TODO: Expose gnatsd parsing into server options
	// (cls) This is a development placeholder until gnatsd
	// options parsing is exposed, if we go that way.
	flag.IntVar(&opts.Port, "port", 0, "Port to listen on.")
	flag.IntVar(&opts.Port, "p", 0, "Port to listen on.")
	flag.StringVar(&opts.Host, "addr", "", "Network host to listen on.")
	flag.StringVar(&opts.Host, "a", "", "Network host to listen on.")
	flag.StringVar(&opts.Host, "net", "", "Network host to listen on.")
	flag.BoolVar(&opts.Debug, "D", false, "Enable Debug logging.")
	flag.BoolVar(&opts.Debug, "debug", false, "Enable Debug logging.")
	flag.BoolVar(&opts.Trace, "V", false, "Enable Trace logging.")
	flag.BoolVar(&opts.Trace, "trace", false, "Enable Trace logging.")
	flag.BoolVar(&debugAndTrace, "DV", false, "Enable Debug and Trace logging.")
	flag.BoolVar(&opts.Logtime, "T", true, "Timestamp log entries.")
	flag.BoolVar(&opts.Logtime, "logtime", true, "Timestamp log entries.")
	flag.StringVar(&opts.Username, "user", "", "Username required for connection.")
	flag.StringVar(&opts.Password, "pass", "", "Password required for connection.")
	flag.StringVar(&opts.Authorization, "auth", "", "Authorization token required for connection.")
	flag.IntVar(&opts.HTTPPort, "m", 0, "HTTP Port for /varz, /connz endpoints.")
	flag.IntVar(&opts.HTTPPort, "http_port", 0, "HTTP Port for /varz, /connz endpoints.")
	flag.IntVar(&opts.HTTPSPort, "ms", 0, "HTTPS Port for /varz, /connz endpoints.")
	flag.IntVar(&opts.HTTPSPort, "https_port", 0, "HTTPS Port for /varz, /connz endpoints.")
	flag.StringVar(&configFile, "c", "", "Configuration file.")
	flag.StringVar(&configFile, "config", "", "Configuration file.")
	flag.StringVar(&opts.PidFile, "P", "", "File to store process pid.")
	flag.StringVar(&opts.PidFile, "pid", "", "File to store process pid.")
	flag.StringVar(&opts.LogFile, "l", "", "File to store logging output.")
	flag.StringVar(&opts.LogFile, "log", "", "File to store logging output.")
	flag.BoolVar(&opts.Syslog, "s", false, "Enable syslog as log method.")
	flag.BoolVar(&opts.Syslog, "syslog", false, "Enable syslog as log method..")
	flag.StringVar(&opts.RemoteSyslog, "r", "", "Syslog server addr (udp://localhost:514).")
	flag.StringVar(&opts.RemoteSyslog, "remote_syslog", "", "Syslog server addr (udp://localhost:514).")
	flag.BoolVar(&showVersion, "version", false, "Print version information.")
	flag.BoolVar(&showVersion, "v", false, "Print version information.")
	flag.IntVar(&opts.ProfPort, "profile", 0, "Profiling HTTP port")
	flag.StringVar(&opts.RoutesStr, "routes", "", "Routes to actively solicit a connection.")
	flag.StringVar(&opts.ClusterListenStr, "cluster", "", "Cluster url from which members can solicit routes.")
	flag.StringVar(&opts.ClusterListenStr, "cluster_listen", "", "Cluster url from which members can solicit routes.")
	flag.BoolVar(&showTLSHelp, "help_tls", false, "TLS help.")
	flag.BoolVar(&opts.TLS, "tls", false, "Enable TLS.")
	flag.BoolVar(&opts.TLSVerify, "tlsverify", false, "Enable TLS with client verification.")
	flag.StringVar(&opts.TLSCert, "tlscert", "", "Server certificate file.")
	flag.StringVar(&opts.TLSKey, "tlskey", "", "Private key for server certificate.")
	flag.StringVar(&opts.TLSCaCert, "tlscacert", "", "Client certificate CA for verification.")

	// Not public per se, will be replaced with dynamic system, but can be used to lower memory footprint when
	// lots of connections present.
	flag.IntVar(&opts.BufSize, "bs", 0, "Read/Write buffer size per client connection.")

	flag.Usage = natsd.Usage

	flag.Parse()

	// Show version and exit
	if showVersion {
		fmt.Printf("stan-server version %s, ", stan.Version)
		natsd.PrintServerAndExit()
	}

	if showTLSHelp {
		natsd.PrintTLSHelpAndDie()
	}

	// One flag can set multiple options.
	if debugAndTrace {
		opts.Trace, opts.Debug = true, true
	}
}

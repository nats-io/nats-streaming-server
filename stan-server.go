// Copyright 2016 Apcera Inc. All rights reserved.

package main

import (
	"flag"
	"runtime"

	"fmt"
	natsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/stan"
	stand "github.com/nats-io/stan-server/server"
	"time"
)

func main() {

	// Parse flags
	sOpts, nOpts := parseFlags()
	stand.EnableDefaultLogger(&nOpts)
	stand.Noticef("Starting stan-server[%s] version %s", sOpts.ID, stan.Version)
	stand.RunServerWithOpts(&sOpts, &nOpts)

	runtime.Goexit()
}

func parseFlags() (stand.ServerOptions, natsd.Options) {

	var showVersion bool
	var debugAndTrace bool
	var showTLSHelp bool
	var configFile string

	stanOpts := stand.DefaultServerOptions
	flag.StringVar(&stanOpts.ID, "cluster_id", stand.DefaultClusterID, "Cluster ID.")

	natsOpts := natsd.Options{}

	// TODO: Expose gnatsd parsing into server options
	// (cls) This is a development placeholder until gnatsd
	// options parsing is exposed, if we go that way.
	flag.IntVar(&natsOpts.Port, "port", 0, "Port to listen on.")
	flag.IntVar(&natsOpts.Port, "p", 0, "Port to listen on.")
	flag.StringVar(&natsOpts.Host, "addr", "", "Network host to listen on.")
	flag.StringVar(&natsOpts.Host, "a", "", "Network host to listen on.")
	flag.StringVar(&natsOpts.Host, "net", "", "Network host to listen on.")
	flag.BoolVar(&natsOpts.Debug, "D", false, "Enable Debug logging.")
	flag.BoolVar(&natsOpts.Debug, "debug", false, "Enable Debug logging.")
	flag.BoolVar(&natsOpts.Trace, "V", false, "Enable Trace logging.")
	flag.BoolVar(&natsOpts.Trace, "trace", false, "Enable Trace logging.")
	flag.BoolVar(&debugAndTrace, "DV", false, "Enable Debug and Trace logging.")
	flag.BoolVar(&natsOpts.Logtime, "T", true, "Timestamp log entries.")
	flag.BoolVar(&natsOpts.Logtime, "logtime", true, "Timestamp log entries.")
	flag.StringVar(&natsOpts.Username, "user", "", "Username required for connection.")
	flag.StringVar(&natsOpts.Password, "pass", "", "Password required for connection.")
	flag.StringVar(&natsOpts.Authorization, "auth", "", "Authorization token required for connection.")
	flag.IntVar(&natsOpts.HTTPPort, "m", 0, "HTTP Port for /varz, /connz endpoints.")
	flag.IntVar(&natsOpts.HTTPPort, "http_port", 0, "HTTP Port for /varz, /connz endpoints.")
	flag.IntVar(&natsOpts.HTTPSPort, "ms", 0, "HTTPS Port for /varz, /connz endpoints.")
	flag.IntVar(&natsOpts.HTTPSPort, "https_port", 0, "HTTPS Port for /varz, /connz endpoints.")
	flag.StringVar(&configFile, "c", "", "Configuration file.")
	flag.StringVar(&configFile, "config", "", "Configuration file.")
	flag.StringVar(&natsOpts.PidFile, "P", "", "File to store process pid.")
	flag.StringVar(&natsOpts.PidFile, "pid", "", "File to store process pid.")
	flag.StringVar(&natsOpts.LogFile, "l", "", "File to store logging output.")
	flag.StringVar(&natsOpts.LogFile, "log", "", "File to store logging output.")
	flag.BoolVar(&natsOpts.Syslog, "s", false, "Enable syslog as log method.")
	flag.BoolVar(&natsOpts.Syslog, "syslog", false, "Enable syslog as log method..")
	flag.StringVar(&natsOpts.RemoteSyslog, "r", "", "Syslog server addr (udp://localhost:514).")
	flag.StringVar(&natsOpts.RemoteSyslog, "remote_syslog", "", "Syslog server addr (udp://localhost:514).")
	flag.BoolVar(&showVersion, "version", false, "Print version information.")
	flag.BoolVar(&showVersion, "v", false, "Print version information.")
	flag.IntVar(&natsOpts.ProfPort, "profile", 0, "Profiling HTTP port")
	flag.StringVar(&natsOpts.RoutesStr, "routes", "", "Routes to actively solicit a connection.")
	flag.StringVar(&natsOpts.ClusterListenStr, "cluster", "", "Cluster url from which members can solicit routes.")
	flag.StringVar(&natsOpts.ClusterListenStr, "cluster_listen", "", "Cluster url from which members can solicit routes.")
	flag.BoolVar(&showTLSHelp, "help_tls", false, "TLS help.")
	flag.BoolVar(&natsOpts.TLS, "tls", false, "Enable TLS.")
	flag.BoolVar(&natsOpts.TLSVerify, "tlsverify", false, "Enable TLS with client verification.")
	flag.StringVar(&natsOpts.TLSCert, "tlscert", "", "Server certificate file.")
	flag.StringVar(&natsOpts.TLSKey, "tlskey", "", "Private key for server certificate.")
	flag.StringVar(&natsOpts.TLSCaCert, "tlscacert", "", "Client certificate CA for verification.")

	// Not public per se, will be replaced with dynamic system, but can be used to lower memory footprint when
	// lots of connections present.
	flag.IntVar(&natsOpts.BufSize, "bs", 0, "Read/Write buffer size per client connection.")

	flag.Usage = stand.Usage
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
		natsOpts.Trace, natsOpts.Debug = true, true
	}

	return stanOpts, natsOpts
}

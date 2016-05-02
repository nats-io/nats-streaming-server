// Copyright 2016 Apcera Inc. All rights reserved.

package main

import (
	"flag"
	"os"
	"runtime"
	"strings"

	"fmt"

	natsd "github.com/nats-io/gnatsd/server"
	stand "github.com/nats-io/stan-server/server"
	"github.com/nats-io/stan-server/stores"
)

func main() {

	// Parse flags
	sOpts, nOpts := parseFlags()
	stand.ConfigureLogger(sOpts, nOpts)
	stand.RunServerWithOpts(sOpts, nOpts)

	runtime.Goexit()
}

func parseFlags() (*stand.Options, *natsd.Options) {

	// STAN options
	var stanDebugAndTrace bool

	stanOpts := stand.GetDefaultOptions()
	flag.StringVar(&stanOpts.ID, "cluster_id", stand.DefaultClusterID, "Cluster ID.")
	flag.StringVar(&stanOpts.StoreType, "store", stores.TypeMemory, fmt.Sprintf("Store type: (%s|%s)", stores.TypeMemory, stores.TypeFile))
	flag.StringVar(&stanOpts.FilestoreDir, "dir", "", "Root directory")
	flag.IntVar(&stanOpts.MaxChannels, "max_channels", stand.DefaultChannelLimit, "Max number of channels")
	flag.IntVar(&stanOpts.MaxSubscriptions, "max_subs", stand.DefaultSubStoreLimit, "Max number of subscriptions per channel")
	flag.IntVar(&stanOpts.MaxMsgs, "max_msgs", stand.DefaultMsgStoreLimit, "Max number of messages per channel")
	flag.Uint64Var(&stanOpts.MaxBytes, "max_bytes", stand.DefaultMsgSizeStoreLimit, "Max messages total size per channel")
	flag.BoolVar(&stanOpts.Debug, "SD", false, "Enable STAN Debug logging.")
	flag.BoolVar(&stanOpts.Debug, "stan_debug", false, "Enable STAN Debug logging.")
	flag.BoolVar(&stanOpts.Trace, "SV", false, "Enable STAN Trace logging.")
	flag.BoolVar(&stanOpts.Trace, "stan_trace", false, "Enable STAN Trace logging.")
	flag.BoolVar(&stanDebugAndTrace, "SDV", false, "Enable STAN Debug and Trace logging.")

	// NATS options
	var showVersion bool
	var natsDebugAndTrace bool
	var showTLSHelp bool
	var configFile string

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
	flag.BoolVar(&natsDebugAndTrace, "DV", false, "Enable Debug and Trace logging.")
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

	flag.Usage = stand.Usage
	flag.Parse()

	// Show version and exit
	if showVersion {
		fmt.Printf("stan-server version %s, ", stand.VERSION)
		natsd.PrintServerAndExit()
	}

	//
	// NATS server option special handling
	//
	if showTLSHelp {
		natsd.PrintTLSHelpAndDie()
	}

	// One flag can set multiple options.
	if natsDebugAndTrace {
		natsOpts.Trace, natsOpts.Debug = true, true
	}

	// for now, key off of one flag - the NATS flag to disable logging.
	natsOpts.NoLog = false

	//
	// STAN server special option handling
	//
	// Ensure some options are set based on selected store type
	checkStoreOpts(stanOpts)

	// One flag can set multiple options.
	if stanDebugAndTrace {
		stanOpts.Trace, stanOpts.Debug = true, true
	}

	return stanOpts, &natsOpts
}

func checkStoreOpts(opts *stand.Options) {
	// Convert the user input to upper case
	storeType := strings.ToUpper(opts.StoreType)

	// If FILE, check some parameters
	if storeType == stores.TypeFile {
		if opts.FilestoreDir == "" {
			fmt.Printf("\nFor %v stores, option \"-dir\" must be specified\n", stores.TypeFile)
			flag.Usage()
			os.Exit(0)
		}
	}
}

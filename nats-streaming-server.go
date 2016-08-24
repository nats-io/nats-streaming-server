// Copyright 2016 Apcera Inc. All rights reserved.

package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"strings"

	"fmt"

	natsd "github.com/nats-io/gnatsd/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats-streaming-server/stores"
)

var usageStr = `
Usage: nats-streaming-server [options]

Streaming Server Options:
    -cid, --cluster_id  <cluster ID> Cluster ID (default: test-cluster)
    -st,  --store <type>             Store type: MEMORY|FILE (default: MEMORY)
          --dir <directory>          For FILE store type, this is the root directory
    -mc,  --max_channels <number>    Max number of channels (aka subjects, topics, etc...)
    -msu, --max_subs <number>        Max number of subscriptions per channel
    -mm,  --max_msgs <number>        Max number of messages per channel
    -mb,  --max_bytes <number>       Max messages total size per channel
    -ns,  --nats_server <url>        Connect to this external NATS Server (embedded otherwise)

Streaming Server TLS Options:
    -secure                          Use a TLS connection to the NATS server without
                                     verification; weaker than specifying certificates.
    -tls_client_key                  Client key for the streaming server
    -tls_client_cert                 Client certificate for the streaming server
    -tls_client_cacert               Client certificate CA for the streaming server

Streaming Server Logging Options:
    -SD, --stan_debug                Enable STAN debugging output
    -SV, --stan_trace                Trace the raw STAN protocol
    -SDV                             Debug and trace STAN
    (See additional NATS logging options below)

Embedded NATS Server Options:
    -a, --addr <host>                Bind to host address (default: 0.0.0.0)
    -p, --port <port>                Use port for clients (default: 4222)
    -P, --pid <file>                 File to store PID
    -m, --http_port <port>           Use port for http monitoring
    -ms,--https_port <port>          Use port for https monitoring
    -c, --config <file>              Configuration file

Logging Options:
    -l, --log <file>                 File to redirect log output
    -T, --logtime                    Timestamp log entries (default: true)
    -s, --syslog                     Enable syslog as log method
    -r, --remote_syslog <addr>       Syslog server addr (udp://localhost:514)
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol
    -DV                              Debug and trace

Authorization Options:
        --user <user>                User required for connections
        --pass <password>            Password required for connections
        --auth <token>               Authorization token required for connections

TLS Options:
        --tls                        Enable TLS, do not verify clients (default: false)
        --tlscert <file>             Server certificate file
        --tlskey <file>              Private key for server certificate
        --tlsverify                  Enable TLS, very client certificates
        --tlscacert <file>           Client certificate CA for verification

NATS Clustering Options:
        --routes <rurl-1, rurl-2>    Routes to solicit and connect
        --cluster <cluster-url>      Cluster URL for solicited routes

Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version
        --help_tls                   TLS help.
`

// usage will print out the flag options for the server.
func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func main() {

	// Parse flags
	sOpts, nOpts := parseFlags()
	// override the NoSigs for NATS since we have our own signal handler below
	nOpts.NoSigs = true
	stand.ConfigureLogger(sOpts, nOpts)
	s := stand.RunServerWithOpts(sOpts, nOpts)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		s.Shutdown()
		os.Exit(0)
	}()

	runtime.Goexit()
}

func parseFlags() (*stand.Options, *natsd.Options) {

	// STAN options
	var stanDebugAndTrace bool

	stanOpts := stand.GetDefaultOptions()
	flag.StringVar(&stanOpts.ID, "cluster_id", stand.DefaultClusterID, "Cluster ID.")
	flag.StringVar(&stanOpts.ID, "cid", stand.DefaultClusterID, "Cluster ID.")
	flag.StringVar(&stanOpts.StoreType, "store", stores.TypeMemory, fmt.Sprintf("Store type: (%s|%s)", stores.TypeMemory, stores.TypeFile))
	flag.StringVar(&stanOpts.StoreType, "st", stores.TypeMemory, fmt.Sprintf("Store type: (%s|%s)", stores.TypeMemory, stores.TypeFile))
	flag.StringVar(&stanOpts.FilestoreDir, "dir", "", "Root directory")
	flag.IntVar(&stanOpts.MaxChannels, "max_channels", stand.DefaultChannelLimit, "Max number of channels")
	flag.IntVar(&stanOpts.MaxChannels, "mc", stand.DefaultChannelLimit, "Max number of channels")
	flag.IntVar(&stanOpts.MaxSubscriptions, "max_subs", stand.DefaultSubStoreLimit, "Max number of subscriptions per channel")
	flag.IntVar(&stanOpts.MaxSubscriptions, "msu", stand.DefaultSubStoreLimit, "Max number of subscriptions per channel")
	flag.IntVar(&stanOpts.MaxMsgs, "max_msgs", stand.DefaultMsgStoreLimit, "Max number of messages per channel")
	flag.IntVar(&stanOpts.MaxMsgs, "mm", stand.DefaultMsgStoreLimit, "Max number of messages per channel")
	flag.Uint64Var(&stanOpts.MaxBytes, "max_bytes", stand.DefaultMsgSizeStoreLimit, "Max messages total size per channel")
	flag.Uint64Var(&stanOpts.MaxBytes, "mb", stand.DefaultMsgSizeStoreLimit, "Max messages total size per channel")
	flag.BoolVar(&stanOpts.Debug, "SD", false, "Enable STAN Debug logging.")
	flag.BoolVar(&stanOpts.Debug, "stan_debug", false, "Enable STAN Debug logging.")
	flag.BoolVar(&stanOpts.Trace, "SV", false, "Enable STAN Trace logging.")
	flag.BoolVar(&stanOpts.Trace, "stan_trace", false, "Enable STAN Trace logging.")
	flag.BoolVar(&stanDebugAndTrace, "SDV", false, "Enable STAN Debug and Trace logging.")
	flag.BoolVar(&stanOpts.Secure, "secure", false, "Enables TLS secure connection that skips server verification.")
	flag.StringVar(&stanOpts.ClientCert, "tls_client_cert", "", "Path to a client certificate file")
	flag.StringVar(&stanOpts.ClientKey, "tls_client_key", "", "Path to a client key file")
	flag.StringVar(&stanOpts.ClientCA, "tls_client_cacert", "", "Path to a client CA file")
	flag.StringVar(&stanOpts.NATSServerURL, "nats_server", "", "URL of the NATS Server to connect to (embedded by default)")
	flag.StringVar(&stanOpts.NATSServerURL, "ns", "", "URL of the NATS Server to connect to (embedded by default)")
	flag.BoolVar(&stanOpts.FileStoreOpts.CompactEnabled, "file_compact_enabled", stores.DefaultFileStoreOptions.CompactEnabled, "Enable file compaction")
	flag.IntVar(&stanOpts.FileStoreOpts.CompactFragmentation, "file_compact_frag", stores.DefaultFileStoreOptions.CompactFragmentation, "File fragmentation threshold for compaction")
	flag.IntVar(&stanOpts.FileStoreOpts.CompactInterval, "file_compact_interval", stores.DefaultFileStoreOptions.CompactInterval, "Minimum interval (in seconds) between file compactions")
	flag.Int64Var(&stanOpts.FileStoreOpts.CompactMinFileSize, "file_compact_min_size", stores.DefaultFileStoreOptions.CompactMinFileSize, "Minimum file size for compaction")
	flag.IntVar(&stanOpts.FileStoreOpts.BufferSize, "file_buffer_size", stores.DefaultFileStoreOptions.BufferSize, "File buffer size (in bytes)")
	flag.BoolVar(&stanOpts.FileStoreOpts.DoCRC, "file_crc", stores.DefaultFileStoreOptions.DoCRC, "Enable file CRC-32 checksum")
	flag.Int64Var(&stanOpts.FileStoreOpts.CRCPolynomial, "file_crc_poly", stores.DefaultFileStoreOptions.CRCPolynomial, "Polynomial used to make the table used for CRC-32 checksum")
	flag.BoolVar(&stanOpts.FileStoreOpts.DoSync, "file_sync", stores.DefaultFileStoreOptions.DoSync, "Enable File.Sync on Flush")
	flag.BoolVar(&stanOpts.FileStoreOpts.CacheMsgs, "file_cache", stores.DefaultFileStoreOptions.CacheMsgs, "Enable messages caching")
	flag.IntVar(&stanOpts.IOBatchSize, "io_batch_size", stand.DefaultIOBatchSize, "# of message to batch in flushing io")
	flag.Int64Var(&stanOpts.IOSleepTime, "io_sleep_time", stand.DefaultIOSleepTime, "duration the server waits for more messages (in micro-seconds, 0 to disable)")
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

	flag.Usage = usage
	flag.Parse()

	// Show version and exit
	if showVersion {
		fmt.Printf("nats-streaming-server version %s, ", stand.VERSION)
		natsd.PrintServerAndExit()
	}

	//
	// NATS server option special handling
	//
	if showTLSHelp {
		natsd.PrintTLSHelpAndDie()
	}

	// Parse config if given
	if configFile != "" {
		fileOpts, err := natsd.ProcessConfigFile(configFile)
		if err != nil {
			natsd.PrintAndDie(err.Error())
		}
		natsOpts = *natsd.MergeOptions(fileOpts, &natsOpts)
	}

	// Remove any host/ip that points to itself in Route
	newroutes, err := natsd.RemoveSelfReference(natsOpts.ClusterPort, natsOpts.Routes)
	if err != nil {
		natsd.PrintAndDie(err.Error())
	}
	natsOpts.Routes = newroutes

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

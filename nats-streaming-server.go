// Copyright 2017 Apcera Inc. All rights reserved.

//go:generate protoc -I=. -I=$GOPATH/src  --gofast_out=. ./spb/protocol.proto

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/nats-io/gnatsd/conf"
	natsd "github.com/nats-io/gnatsd/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats-streaming-server/stores"
)

var usageStr = `
Usage: nats-streaming-server [options]

Streaming Server Options:
    -cid, --cluster_id  <string>     Cluster ID (default: test-cluster)
    -st,  --store <string>           Store type: MEMORY|FILE (default: MEMORY)
          --dir <string>             For FILE store type, this is the root directory
    -mc,  --max_channels <int>       Max number of channels (0 for unlimited)
    -msu, --max_subs <int>           Max number of subscriptions per channel (0 for unlimited)
    -mm,  --max_msgs <int>           Max number of messages per channel (0 for unlimited)
    -mb,  --max_bytes <size>         Max messages total size per channel (0 for unlimited)
    -ma,  --max_age <duration>       Max duration a message can be stored ("0s" for unlimited)
    -ns,  --nats_server <string>     Connect to this external NATS Server URL (embedded otherwise)
    -sc,  --stan_config <string>     Streaming server configuration file
    -hbi, --hb_interval <duration>   Interval at which server sends heartbeat to a client
    -hbt, --hb_timeout <duration>    How long server waits for a heartbeat response
    -hbf, --hb_fail_count <int>      Number of failed heartbeats before server closes the client connection
          --ack_subs <int>           Number of internal subscriptions handling incoming ACKs (0 means one per client's subscription)
          --ft_group <string>        Name of the FT Group. A group can be 2 or more servers with a single active server and all sharing the same datastore.

Streaming Server Clustering Options:
    --cluster_node_id <string>  ID of the node within the cluster.
    --cluster_peers <string>    List of cluster peer IDs.
    --cluster_log_path <string> Directory to store log replication data.

Streaming Server File Store Options:
    --file_compact_enabled <bool>        Enable file compaction
    --file_compact_frag <int>            File fragmentation threshold for compaction
    --file_compact_interval <int>        Minimum interval (in seconds) between file compactions
    --file_compact_min_size <size>       Minimum file size for compaction
    --file_buffer_size <size>            File buffer size (in bytes)
    --file_crc <bool>                    Enable file CRC-32 checksum
    --file_crc_poly <int>                Polynomial used to make the table used for CRC-32 checksum
    --file_sync <bool>                   Enable File.Sync on Flush
    --file_slice_max_msgs <int>          Maximum number of messages per file slice (subject to channel limits)
    --file_slice_max_bytes <size>        Maximum file slice size - including index file (subject to channel limits)
    --file_slice_max_age <duration>      Maximum file slice duration starting when the first message is stored (subject to channel limits)
    --file_slice_archive_script <string> Path to script to use if you want to archive a file slice being removed
    --file_fds_limit <int>               Store will try to use no more file descriptors than this given limit
    --file_parallel_recovery <int>       On startup, number of channels that can be recovered in parallel

Streaming Server TLS Options:
    -secure <bool>                   Use a TLS connection to the NATS server without
                                     verification; weaker than specifying certificates.
    -tls_client_key <string>         Client key for the streaming server
    -tls_client_cert <string>        Client certificate for the streaming server
    -tls_client_cacert <string>      Client certificate CA for the streaming server

Streaming Server Logging Options:
    -SD, --stan_debug=<bool>         Enable STAN debugging output
    -SV, --stan_trace=<bool>         Trace the raw STAN protocol
    -SDV                             Debug and trace STAN
    (See additional NATS logging options below)

Embedded NATS Server Options:
    -a, --addr <string>              Bind to host address (default: 0.0.0.0)
    -p, --port <int>                 Use port for clients (default: 4222)
    -P, --pid <string>               File to store PID
    -m, --http_port <int>            Use port for http monitoring
    -ms,--https_port <int>           Use port for https monitoring
    -c, --config <string>            Configuration file

Logging Options:
    -l, --log <string>               File to redirect log output
    -T, --logtime=<bool>             Timestamp log entries (default: true)
    -s, --syslog <string>            Enable syslog as log method
    -r, --remote_syslog <string>     Syslog server addr (udp://localhost:514)
    -D, --debug=<bool>               Enable debugging output
    -V, --trace=<bool>               Trace the raw protocol
    -DV                              Debug and trace

Authorization Options:
        --user <string>              User required for connections
        --pass <string>              Password required for connections
        --auth <string>              Authorization token required for connections

TLS Options:
        --tls=<bool>                 Enable TLS, do not verify clients (default: false)
        --tlscert <string>           Server certificate file
        --tlskey <string>            Private key for server certificate
        --tlsverify=<bool>           Enable TLS, verify client certificates
        --tlscacert <string>         Client certificate CA for verification

NATS Clustering Options:
        --routes <string, ...>       Routes to solicit and connect
        --cluster <string>           Cluster URL for solicited routes

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
	// Force the streaming server to setup its own signal handler
	sOpts.HandleSignals = true
	// override the NoSigs for NATS since Streaming has its own signal handler
	nOpts.NoSigs = true
	// Without this option set to true, the logger is not configured.
	sOpts.EnableLogging = true
	if _, err := stand.RunServerWithOpts(sOpts, nOpts); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	runtime.Goexit()
}

func parseFlags() (*stand.Options, *natsd.Options) {

	// STAN options
	var (
		stanDebugAndTrace bool
		stanConfigFile    string
		stanClusterPeers  string
	)

	// Define the flags for STAN. We use the Usage (last field)
	// as the actual Options name. We will then use reflection
	// to apply any command line option to stanOpts, overriding
	// defaults and options set during file parsing.
	flag.String("cluster_id", stand.DefaultClusterID, "stan.ID")
	flag.String("cid", stand.DefaultClusterID, "stan.ID")
	flag.String("store", stores.TypeMemory, "stan.StoreType")
	flag.String("st", stores.TypeMemory, "stan.StoreType")
	flag.String("dir", "", "stan.FilestoreDir")
	flag.Int("max_channels", stores.DefaultStoreLimits.MaxChannels, "stan.MaxChannels")
	flag.Int("mc", stores.DefaultStoreLimits.MaxChannels, "stan.MaxChannels")
	flag.Int("max_subs", stores.DefaultStoreLimits.MaxSubscriptions, "stan.MaxSubscriptions")
	flag.Int("msu", stores.DefaultStoreLimits.MaxSubscriptions, "stan.MaxSubscriptions")
	flag.Int("max_msgs", stores.DefaultStoreLimits.MaxMsgs, "stan.MaxMsgs")
	flag.Int("mm", stores.DefaultStoreLimits.MaxMsgs, "stan.MaxMsgs")
	flag.String("max_bytes", fmt.Sprintf("%v", stores.DefaultStoreLimits.MaxBytes), "stan.MaxBytes")
	flag.String("mb", fmt.Sprintf("%v", stores.DefaultStoreLimits.MaxBytes), "stan.MaxBytes")
	flag.String("max_age", "0s", "stan.MaxAge")
	flag.String("ma", "0s", "stan.MaxAge")
	flag.String("hbi", stand.DefaultHeartBeatInterval.String(), "stan.ClientHBInterval")
	flag.String("hb_interval", stand.DefaultHeartBeatInterval.String(), "stan.ClientHBInterval")
	flag.String("hbt", stand.DefaultClientHBTimeout.String(), "stan.ClientHBTimeout")
	flag.String("hb_timeout", stand.DefaultClientHBTimeout.String(), "stan.ClientHBTimeout")
	flag.Int("hbf", stand.DefaultMaxFailedHeartBeats, "stan.ClientHBFailCount")
	flag.Int("hb_fail_count", stand.DefaultMaxFailedHeartBeats, "stan.ClientHBFailCount")
	flag.Bool("SD", false, "stan.Debug")
	flag.Bool("stan_debug", false, "stan.Debug")
	flag.Bool("SV", false, "stan.Trace")
	flag.Bool("stan_trace", false, "stan.Trace")
	flag.BoolVar(&stanDebugAndTrace, "SDV", false, "")
	flag.Bool("secure", false, "stan.Secure")
	flag.String("tls_client_cert", "", "stan.ClientCert")
	flag.String("tls_client_key", "", "stan.ClientKey")
	flag.String("tls_client_cacert", "", "stan.ClientCA")
	flag.String("nats_server", "", "stan.NATSServerURL")
	flag.String("ns", "", "stan.NATSServerURL")
	flag.StringVar(&stanConfigFile, "sc", "", "")
	flag.StringVar(&stanConfigFile, "stan_config", "", "")
	flag.Int("ack_subs", 0, "stan.AckSubsPoolSize")
	flag.Bool("file_compact_enabled", stores.DefaultFileStoreOptions.CompactEnabled, "stan.FileStoreOpts.CompactEnabled")
	flag.Int("file_compact_frag", stores.DefaultFileStoreOptions.CompactFragmentation, "stan.FileStoreOpts.CompactFragmentation")
	flag.Int("file_compact_interval", stores.DefaultFileStoreOptions.CompactInterval, "stan.FileStoreOpts.CompactInterval")
	flag.String("file_compact_min_size", fmt.Sprintf("%v", stores.DefaultFileStoreOptions.CompactMinFileSize), "stan.FileStoreOpts.CompactMinFileSize")
	flag.String("file_buffer_size", fmt.Sprintf("%v", stores.DefaultFileStoreOptions.BufferSize), "stan.FileStoreOpts.BufferSize")
	flag.Bool("file_crc", stores.DefaultFileStoreOptions.DoCRC, "stan.FileStoreOpts.DoCRC")
	flag.Int64("file_crc_poly", stores.DefaultFileStoreOptions.CRCPolynomial, "stan.FileStoreOpts.CRCPolynomial")
	flag.Bool("file_sync", stores.DefaultFileStoreOptions.DoSync, "stan.FileStoreOpts.DoSync")
	flag.Int("file_slice_max_msgs", stores.DefaultFileStoreOptions.SliceMaxMsgs, "stan.FileStoreOpts.SliceMaxMsgs")
	flag.String("file_slice_max_bytes", fmt.Sprintf("%v", stores.DefaultFileStoreOptions.SliceMaxBytes), "stan.FileStoreOpts.SliceMaxBytes")
	flag.String("file_slice_max_age", "0s", "stan.FileStoreOpts.SliceMaxAge")
	flag.String("file_slice_archive_script", "", "stan.FileStoreOpts.SliceArchiveScript")
	flag.Int64("file_fds_limit", stores.DefaultFileStoreOptions.FileDescriptorsLimit, "stan.FileStoreOpts.FileDescriptorsLimit")
	flag.Int("file_parallel_recovery", stores.DefaultFileStoreOptions.ParallelRecovery, "stan.FileStoreOpts.ParallelRecovery")
	flag.Int("io_batch_size", stand.DefaultIOBatchSize, "stan.IOBatchSize")
	flag.Int64("io_sleep_time", stand.DefaultIOSleepTime, "stan.IOSleepTime")
	flag.String("ft_group", "", "stan.FTGroupName")
	flag.String("cluster_node_id", "", "stan.ClusterNodeID")
	flag.StringVar(&stanClusterPeers, "cluster_peers", "", "")
	flag.String("cluster_log_path", "", "stan.RaftLogPath")

	// NATS options
	var showVersion bool
	var natsDebugAndTrace bool
	var showTLSHelp bool
	var gnatsdConfigFile string

	// TODO: Expose gnatsd parsing into server options
	// (cls) This is a development placeholder until gnatsd
	// options parsing is exposed, if we go that way.
	//
	// IMPORTANT: Do not use Usage field (last) since this is
	// used to do reflection. Note that usage is defined in
	// usageStr anyway.
	flag.Int("port", 0, "nats.Port")
	flag.Int("p", 0, "nats.Port")
	flag.String("addr", "", "nats.Host")
	flag.String("a", "", "nats.Host")
	flag.String("net", "", "nats.Host")
	flag.Bool("D", false, "nats.Debug")
	flag.Bool("debug", false, "nats.Debug")
	flag.Bool("V", false, "nats.Trace")
	flag.Bool("trace", false, "nats.Trace")
	flag.BoolVar(&natsDebugAndTrace, "DV", false, "")
	flag.Bool("T", true, "nats.Logtime")
	flag.Bool("logtime", true, "nats.Logtime")
	flag.String("user", "", "nats.Username")
	flag.String("pass", "", "nats.Password")
	flag.String("auth", "", "nats.Authorization")
	flag.Int("m", 0, "nats.HTTPPort")
	flag.Int("http_port", 0, "nats.HTTPPort")
	flag.Int("ms", 0, "nats.HTTPSPort")
	flag.Int("https_port", 0, "nats.HTTPSPort")
	flag.StringVar(&gnatsdConfigFile, "c", "", "")
	flag.StringVar(&gnatsdConfigFile, "config", "", "")
	flag.String("P", "", "nats.PidFile")
	flag.String("pid", "", "nats.PidFile")
	flag.String("l", "", "nats.LogFile")
	flag.String("log", "", "nats.LogFile")
	flag.Bool("s", false, "nats.Syslog")
	flag.Bool("syslog", false, "nats.Syslog")
	flag.String("r", "", "nats.RemoteSyslog")
	flag.String("remote_syslog", "", "nats.RemoteSyslog")
	flag.BoolVar(&showVersion, "version", false, "")
	flag.BoolVar(&showVersion, "v", false, "")
	flag.Int("profile", 0, "nats.ProfPort")
	flag.String("routes", "", "nats.RoutesStr")
	flag.String("cluster", "", "nats.Cluster.ListenStr")
	flag.String("cluster_listen", "", "nats.Cluster.ListenStr")
	flag.BoolVar(&showTLSHelp, "help_tls", false, "")
	flag.Bool("tls", false, "nats.TLS")
	flag.Bool("tlsverify", false, "nats.TLSVerify")
	flag.String("tlscert", "", "nats.TLSCert")
	flag.String("tlskey", "", "nats.TLSKey")
	flag.String("tlscacert", "", "nats.TLSCaCert")

	flag.Usage = func() {
		fmt.Printf("%s\n", usageStr)
	}
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

	// Process args looking for non-flag options,
	// 'version' and 'help' only for now
	showVersion, showHelp, err := natsd.ProcessCommandLineArgs(flag.CommandLine)
	if err != nil {
		natsd.PrintAndDie(err.Error() + usageStr)
	} else if showVersion {
		fmt.Printf("nats-streaming-server version %s, ", stand.VERSION)
		natsd.PrintServerAndExit()
	} else if showHelp {
		usage()
	}

	stanOpts, natsOpts, err := stand.ProcessConfigFiles(stanConfigFile, gnatsdConfigFile)
	if err != nil {
		natsd.PrintAndDie(fmt.Sprintf("Configuration error: %v", err.Error()))
	}
	// Now apply all parameters provided on the command line.
	if err := overrideWithCmdLineParams(stanOpts, natsOpts); err != nil {
		natsd.PrintAndDie(err.Error())
	}

	// One flag can set multiple options.
	if natsDebugAndTrace {
		natsOpts.Trace, natsOpts.Debug = true, true
	}

	// One flag can set multiple options.
	if stanDebugAndTrace {
		stanOpts.Trace, stanOpts.Debug = true, true
	}

	if len(stanClusterPeers) > 0 {
		stanOpts.ClusterPeers = strings.Split(stanClusterPeers, ",")
		for i, peer := range stanOpts.ClusterPeers {
			stanOpts.ClusterPeers[i] = strings.TrimSpace(peer)
		}
	}

	if stanOpts.RaftLogPath == "" {
		stanOpts.RaftLogPath = filepath.Join(stanOpts.ID, stanOpts.ClusterNodeID)
	}

	return stanOpts, natsOpts
}

// overrideWithCmdLineParams applies the flags passed in the command line
// to the given options structure.
func overrideWithCmdLineParams(sopts *stand.Options, nopts *natsd.Options) error {
	var err error
	flag.Visit(func(f *flag.Flag) {
		if err != nil || f.Usage == "" {
			return
		}
		root := f.Usage[0:5]
		var t reflect.Value
		if root == "stan." {
			t = reflect.ValueOf(sopts).Elem()
		} else if root == "nats." {
			t = reflect.ValueOf(nopts).Elem()
		} else {
			err = fmt.Errorf("unknown options root: %s", root)
			return
		}
		optName := f.Usage[5:]
		var o reflect.Value
		// Lookup for sub structures, ex: FileStoreOpts.CacheMsgs
		if strings.Contains(optName, ".") {
			strs := strings.Split(optName, ".")
			obj := t.FieldByName(strs[0])
			for i := 1; i < len(strs); i++ {
				obj = obj.FieldByName(strs[i])
			}
			o = obj
		} else {
			o = t.FieldByName(optName)
		}
		if !o.IsValid() || !o.CanSet() {
			return
		}
		v, ok := f.Value.(flag.Getter)
		if !ok {
			return
		}
		val := v.Get()
		valKind := reflect.ValueOf(val).Kind()
		switch valKind {
		case reflect.String:
			switch optName {
			// Parameters that can be size are configured as string and we then use
			// gnatsd's configuration parser to convert to a int64.
			case "MaxBytes", "FileStoreOpts.CompactMinFileSize", "FileStoreOpts.BufferSize", "FileStoreOpts.SliceMaxBytes":
				var res map[string]interface{}
				res, err = conf.Parse(fmt.Sprintf("bytes: %v", val))
				if err != nil {
					return
				}
				resVal := res["bytes"]
				if resVal == nil || reflect.TypeOf(resVal).Kind() != reflect.Int64 {
					err = fmt.Errorf("%v should be a size, got '%v'", f.Name, resVal)
					return
				}
				o.SetInt(int64(resVal.(int64)))
			case "MaxAge", "ClientHBInterval", "ClientHBTimeout", "FileStoreOpts.SliceMaxAge":
				var dur time.Duration
				dur, err = time.ParseDuration(val.(string))
				if err != nil {
					return
				}
				o.SetInt(int64(dur))
			case "RoutesStr":
				routesStr := val.(string)
				routesUrls := natsd.RoutesFromStr(routesStr)
				if routesUrls != nil {
					nopts.Routes = routesUrls
					nopts.RoutesStr = routesStr
				}
			default:
				o.SetString(val.(string))
			}
		case reflect.Int:
			o.SetInt(int64(val.(int)))
		case reflect.Int64:
			o.SetInt(val.(int64))
		case reflect.Uint64:
			o.SetUint(val.(uint64))
		case reflect.Bool:
			o.SetBool(val.(bool))
		default:
			panic(fmt.Errorf("Add support for type %v (command line parameter %q)",
				valKind.String(), f.Name))
		}
	})
	return err
}

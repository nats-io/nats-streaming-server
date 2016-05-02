// Copyright 2013-2016 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"os"
)

var usageStr = `
Usage: stan-server [options]

STAN Options:
        -cluster_id  <cluster ID>    Cluster ID (default: test-cluster)
        -store <type>                Store type: MEMORY|FILE (default: MEMORY)
        -dir <directory>             For FILE store type, this is the root directory
        -max_channels <number>       Max number of channels
        -max_subs <number>           Max number of subscriptions per channel
        -max_msgs <number>           Max number of messages per channel
        -max_bytes <number>          Max messages total size per channel

STAN Logging Options:
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

// Usage will print out the flag options for the server.
func Usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

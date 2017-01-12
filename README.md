# NATS Streaming Server

NATS Streaming is an extremely performant, lightweight reliable streaming platform built on [NATS](https://nats.io).

[![License][License-Image]][License-Url] [![ReportCard][ReportCard-Image]][ReportCard-Url] [![Build][Build-Status-Image]][Build-Status-Url] [![Coverage][Coverage-Image]][Coverage-Url]

NATS Streaming provides the following high-level feature set.
- Log based.
- At-Least-Once Delivery model, giving reliable message delivery.
- Rate matched on a per subscription basis.
- Replay/Restart
- Last Value Semantics

### Getting Started

The best way to get the NATS Streaming Server is to use one of the pre-built release binaries which are available for OSX, Linux (x86-64/ARM), Windows. Instructions for using these binaries are on the GitHub releases page.

Of course you can build the latest version of the server from the master branch. The master branch will always build and pass tests, but may not work correctly in your environment. You will first need Go installed on your machine (version 1.5+ is required) to build the NATS server.

See also the NATS Streaming Quickstart [tutorial](https://nats.io/documentation/streaming/nats-streaming-quickstart/).

### Running

The NATS Streaming Server embeds a NATS Server. Starting the server with no argument will give you a server with default settings and a memory based store.

```
> ./nats-streaming-server
[53638] 2016/10/18 10:05:15.718693 [INF] Starting nats-streaming-server[test-cluster] version 0.3.0
[53638] 2016/10/18 10:05:15.721356 [INF] Starting nats-server version 0.9.4
[53638] 2016/10/18 10:05:15.721366 [INF] Listening for client connections on 0.0.0.0:4222
[53638] 2016/10/18 10:05:15.721941 [INF] Server is ready
[53638] 2016/10/18 10:05:16.063714 [INF] STAN: Message store is MEMORY
[53638] 2016/10/18 10:05:16.063726 [INF] STAN: --------- Store Limits ---------
[53638] 2016/10/18 10:05:16.063732 [INF] STAN: Channels:                  100 *
[53638] 2016/10/18 10:05:16.063734 [INF] STAN: -------- channels limits -------
[53638] 2016/10/18 10:05:16.063738 [INF] STAN:   Subscriptions:          1000 *
[53638] 2016/10/18 10:05:16.063741 [INF] STAN:   Messages     :       1000000 *
[53638] 2016/10/18 10:05:16.063786 [INF] STAN:   Bytes        :     976.56 MB *
[53638] 2016/10/18 10:05:16.063789 [INF] STAN:   Age          :     unlimited *
[53638] 2016/10/18 10:05:16.063791 [INF] STAN: --------------------------------
```

The server will be started and listening for client connections on port 4222 (the default) from all available interfaces. The logs will be displayed to stderr as shown above.

Note that you do not need to start the embedded NATS Server. It is started automatically when you run the NATS Streaming Server. See below for details on how you secure the embedded NATS Server.

## Configuring

### Command line arguments

The NATS Streaming Server accepts command line arguments to control its behavior. There is a set of parameters specific to the NATS Streaming Server and some to the embedded NATS Server.

```
Usage: nats-streaming-server [options]

Streaming Server Options:
    -cid, --cluster_id  <cluster ID> Cluster ID (default: test-cluster)
    -st,  --store <type>             Store type: MEMORY|FILE (default: MEMORY)
          --dir <directory>          For FILE store type, this is the root directory
    -mc,  --max_channels <number>    Max number of channels (0 for unlimited)
    -msu, --max_subs <number>        Max number of subscriptions per channel (0 for unlimited)
    -mm,  --max_msgs <number>        Max number of messages per channel (0 for unlimited)
    -mb,  --max_bytes <number>       Max messages total size per channel (0 for unlimited)
    -ma,  --max_age <seconds>        Max duration a message can be stored ("0s" for unlimited)
    -ns,  --nats_server <url>        Connect to this external NATS Server (embedded otherwise)
    -sc,  --stan_config <file>       Streaming server configuration file
    -hbi, --hb_interval <duration>   Interval at which server sends heartbeat to a client
    -hbt, --hb_timeout <duration>    How long server waits for a heartbeat response
    -hbf, --hb_fail_count <number>   Number of failed heartbeats before server closes the client connection

Streaming Server File Store Options:
    --file_compact_enabled           Enable file compaction
    --file_compact_frag              File fragmentation threshold for compaction
    --file_compact_interval <int>    Minimum interval (in seconds) between file compactions
    --file_compact_min_size <int>    Minimum file size for compaction
    --file_buffer_size <int>         File buffer size (in bytes)
    --file_crc                       Enable file CRC-32 checksum
    --file_crc_poly <int>            Polynomial used to make the table used for CRC-32 checksum
    --file_sync                      Enable File.Sync on Flush
    --file_slice_max_msgs            Maximum number of messages per file slice (subject to channel limits)
    --file_slice_max_bytes           Maximum file slice size - including index file (subject to channel limits)
    --file_slice_max_age             Maximum file slice duration starting when the first message is stored (subject to channel limits)
    --file_slice_archive_script      Path to script to use if you want to archive a file slice being removed

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
        --tlsverify                  Enable TLS, verify client certificates
        --tlscacert <file>           Client certificate CA for verification

NATS Clustering Options:
        --routes <rurl-1, rurl-2>    Routes to solicit and connect
        --cluster <cluster-url>      Cluster URL for solicited routes

Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version
        --help_tls                   TLS help.
```

### Configuration file

You can use a configuration file to configure the options specific to the NATS Streaming server.

Use the `-sc` or `-stan_config` command line parameter to specify the file to use.

Note the order in which options are applied during the start of a NATS Streaming server:

1. Start with some reasonable default options.
2. If a configuration file is specified, override those options
  with all options defined in the file. This include options that are defined
  but have no value specified. In this case, the zero value for the type of the
  option will be used.
3. Any command line parameter override all of the previous set options.

In general the configuration parameters are the same as the command line arguments.
But see an example configuration file below for more details:

```
# Define the cluster name.
# Can be id, cid or cluster_id
id: "my_cluster_name"

# Store type
# Can be st, store, store_type or StoreType
# Possible values are file or memory (case insensitive)
store: "file"

# When using a file store, need to provide the root directory.
# Can be dir or datastore
dir: "/path/to/storage"

# Debug flag.
# Can be sd or stand_debug
sd: false

# Trace flag.
# Can be sv or stan_trace
sv: false

# If specified, connects to an external NATS server, otherwise
# starts and embedded server.
# Can be ns, nats_server or nats_server_url
ns: "nats://localhost:4222"

# This flag creates a TLS connection to the server but without
# the need to use a TLS configuration (no NATS server certificate verification).
secure: false

# Interval at which the server sends an heartbeat to a client,
# expressed as a duration.
# Can be hbi, hb_interval, server_to_client_hb_interval
hb_interval: "10s"

# How long the server waits for a heartbeat response from the client
# before considering it a failed hearbeat. Expressed as a duration.
# Can be hbt, hb_timeout, server_to_client_hb_timeout
hb_timeout: "10s"

# Count of failed hearbeats before server closes the client connection.
# The actual total wait is: (fail count + 1) * (hb interval + hb timeout).
# Can be hbf, hb_fail_count, server_to_client_hb_fail_count
hb_fail_count: 2

# Define store limits.
# Can be limits, store_limits or StoreLimits.
# See Store Limits chapter below for more details.
store_limits: {
    # Define maximum number of channels.
    # Can be mc, max_channels or MaxChannels
    max_channels: 100

    # Define maximum number of subscriptions per channel.
    # Can be msu, max_sybs, max_subscriptions or MaxSubscriptions
    max_subs: 100

    # Define maximum number of messages per channel.
    # Can be mm, max_msgs, MaxMsgs, max_count or MaxCount
    max_msgs: 10000

    # Define total size of messages per channel.
    # Can be mb, max_bytes or MaxBytes. Expressed in bytes
    max_bytes: 10240000

    # Define how long messages can stay in the log, expressed
    # as a duration, for example: "24h" or "1h15m", etc...
    # Can be ma, max_age, MaxAge.
    max_age: "24h"
}

# TLS configuration.
tls: {
    client_cert: "/path/to/client/cert_file"
    client_key: "/path/to/client/key_file"
    # Can be client_ca or client_cacert
    client_ca: "/path/to/client/ca_file"
}

# Configure file store specific options.
# Can be file or file_options
file: {
    # Enable/disable file compaction.
    # Can be compact or compact_enabled
    compact: true

    # Define compaction threshold (in percentage)
    # Can be compact_frag or compact_fragmemtation
    compact_frag: 50

    # Define minimum interval between attempts to compact files.
    # Expressed in seconds
    compact_interval: 300

    # Define minimum size of a file before compaction can be attempted
    # Expressed in bytes
    compact_min_size: 10485760

    # Define the size of buffers that can be used to buffer write operations.
    # Expressed in bytes
    buffer_size: 2097152

    # Define if CRC of records should be computed on reads.
    # Can be crc or do_crc
    crc: true

    # You can select the CRC polynomial. Note that changing the value
    # after records have been persisted would result in server failing
    # to start complaining about data corruption.
    crc_poly: 3988292384

    # Define if server should perform "file sync" operations during a flush.
    # Can be sync, do_sync, sync_on_flush
    sync: true

    # Define the file slice maximum number of messages. If set to 0 and a
    # channel count limit is set, then the server will set a slice count
    # limit automatically.
    # Can be slice_max_msgs, slice_max_count, slice_msgs, slice_count
    slice_max_msgs: 10000

    # Define the file slice maximum size (including the size of index file).
    # If set to 0 and a channel size limit is set, then the server will
    # set a slice bytes limit automatically.
    # Expressed in bytes.
    # Can be slice_max_bytes, slice_max_size, slice_bytes, slice_size
    slice_max_bytes: 67108864

    # Define the period of time covered by a file slice, starting at when
    # the first message is stored. If set to 0 and a channel age limit
    # is set, then the server will set a slice age limit automatically.
    # Expressed as a duration, such as "24h", etc..
    # Can be  slice_max_age, slice_age, slice_max_time, slice_time_limit
    slice_max_age: "24h"

    # Define the location and name of a script to be invoked when the
    # server discards a file slice due to limits. The script is invoked
    # with the name of the channel, the name of data and index files.
    # It is the responsability of the script to then remove the unused
    # files.
    # Can be slice_archive_script, slice_archive, slice_script
    slice_archive_script: "/home/nats-streaming/archive/script.sh"
}
```

### Store Limits

The `store_limits` section in the configuration file (or the command line parameters
`-mc`, `-mm`, etc..) allow you to configure the global limits.

These limits somewhat offer some upper bound on the size of the storage. By multiplying
the limits per channel with the maximum number of channels, you will get a total limit.

It is also possible to define specific limits per channel. Here is how:

```
...
store_limits: {
    # Override some global limits
    max_channels: 10
    max_msgs: 10000
    max_bytes: 10485760
    max_age: "1h"

    # Per channel configuration.
    # Can be channels, channels_limits, per_channel, per_channel_limits or ChannelsLimits
    channels: {
        # Configuration for channel "foo"
        "foo": {
            # Possible options are the same than in the store_limits section
            # except for max_channels.
            max_msgs: 300
            max_subs: 50
        }
        "bar": {
            max_msgs:50
            max_bytes:1000
        }
    }
}
...
```

Note the following restrictions:
- The total of channels configured must be less than the store's maximum number of channels.
- No limit can be negative.
- No limit can be greater than its corresponding global limit.

### Limits inheritance

Global limits that are not specified (configuration file or command line parameters)
are inherited from default limits selected by the server.

Per-channel limits that are not explicitly configured inherit from the corresponding
global limit (which can itself be inherited from default limit).

On startup the server displays the store limits. Notice the `*` at the right of a
limit to indicate that the limit was inherited (either from global or default limits).
This is what would be displayed with the above store limits configuration:

```
[53904] 2016/10/18 10:10:50.581799 [INF] STAN: --------- Store Limits ---------
[53904] 2016/10/18 10:10:50.581807 [INF] STAN: Channels:                   10
[53904] 2016/10/18 10:10:50.581810 [INF] STAN: -------- channels limits -------
[53904] 2016/10/18 10:10:50.581816 [INF] STAN:   Subscriptions:          1000 *
[53904] 2016/10/18 10:10:50.581821 [INF] STAN:   Messages     :         10000
[53904] 2016/10/18 10:10:50.581846 [INF] STAN:   Bytes        :      10.00 MB
[53904] 2016/10/18 10:10:50.581859 [INF] STAN:   Age          :        1h0m0s
[53904] 2016/10/18 10:10:50.581867 [INF] STAN: Channel: "foo"
[53904] 2016/10/18 10:10:50.581872 [INF] STAN:   Subscriptions:            50
[53904] 2016/10/18 10:10:50.581877 [INF] STAN:   Messages     :           300
[53904] 2016/10/18 10:10:50.581883 [INF] STAN:   Bytes        :      10.00 MB *
[53904] 2016/10/18 10:10:50.581889 [INF] STAN:   Age          :        1h0m0s *
[53904] 2016/10/18 10:10:50.581893 [INF] STAN: Channel: "bar"
[53904] 2016/10/18 10:10:50.581897 [INF] STAN:   Subscriptions:          1000 *
[53904] 2016/10/18 10:10:50.581902 [INF] STAN:   Messages     :            50
[53904] 2016/10/18 10:10:50.581916 [INF] STAN:   Bytes        :        1000 B
[53904] 2016/10/18 10:10:50.581926 [INF] STAN:   Age          :        1h0m0s *
[53904] 2016/10/18 10:10:50.581930 [INF] STAN: --------------------------------
```


## Securing NATS Streaming Server

### Authorization

If you use basic authorization, that is a single user defined through the configuration file or command line parameters, nothing else is required. The embedded NATS Server is started with those credentials and the NATS Streaming Server uses them.

However, if you define multiple users, then you **must** specify the username and password (or token) corresponding to the NATS Streaming Server's user.

For instance, suppose that your configuration file `server.cfg` has the following content:

```
listen: 127.0.0.1:4233
http: 127.0.0.1:8233

authorization {
    users = [
      {user: alice, password: foo}
      {user: bob,   password: bar}
    ]
}
```
and you start the NATS Streaming Server this way:
```sh
nats-streaming-server -config server.cfg
```
then the server would fail to start. You **must** specify the user used by the streaming server. For instance:
```sh
nats-streaming-server -config server.cfg -user alice -pass foo
```

### TLS

While there are several TLS related parameters to the streaming server, securing the NATS Streaming server's connection is straightforward when you bear in mind that the relationship between the NATS Streaming server and the embedded NATS server is a client server relationship.  To state simply, the streaming server is a client of it's embedded NATS server.

That means two sets of TLS configuration parameters must be used:  TLS server parameters for the embedded NATS server, and TLS client parameters for the streaming server itself.

The streaming server specifies it's TLS client certificates with the following three parameters:
```    
    -tls_client_key              Client key for the streaming server

    -tls_client_cert             Client certificate for the streaming server

    -tls_client_cacert           Client certificate CA for the streaming server
```

These could be the same certificates used with your NATS streaming clients.

The embedded NATS server specifies TLS server certificates with these:

```
        --tlscert <file>             Server certificate file

        --tlskey <file>              Private key for server certificate

        --tlscacert <file>           Client certificate CA for verification
```

The server parameters are used the same way you would secure a typical NATS server. See [here](https://github.com/nats-io/gnatsd#securing-nats).

Proper usage of the NATS Streaming Server requires the use of both client and server parameters.

e.g.:

```sh
nats-streaming-server -tls_client_cert client-cert.pem -tls_client_key client-key.pem -tls_client_cacert ca.pem -tlscert server-cert.pem -tlskey server-key.pem -tlscacert ca.pem
```

Further TLS related functionality can be found in [usage](https://github.com/nats-io/gnatsd#securing-nats), and should specifying cipher suites be required, a configuration file for the embedded NATS server can be passed through the `-config` command line parameter.

## Persistence

By default, the NATS Streaming Server stores its state in memory, which means that if the streaming server is stopped, all state is lost. Still, this level of persistence allows applications to stop and later resume the stream of messages, and protect against applications disconnect (network or applications crash).

### File Store

For a higher level of message delivery, the server should be configured with a file store. NATS Streaming Server comes with a basic file store implementation. Various file store implementations may be added in the future.

To start the server with a file store, you need to provide two parameters:

```sh
nats-streaming-server -store file -dir datastore
```
The parameter `-store` indicates what type of store to use, in this case `file`. The other (`-dir`) indicates in which directory the state should be stored.

The first time the server is started, it will create two files in this directory, one containing some server related information (`server.dat`) another to record clients information (`clients.dat`).

When a streaming client connects, it uses a client identification, which the server registers in this file. When the client disconnects, the client is cleared from this file.

When the client publishes or subscribe to a new subject (also called channel), the server creates a sub-directory whose name is the subject. For instance, if the client subscribes to `foo`, and assuming that you started the server with `-dir datastore`, then you will find a directory called `datastore/foo`. In this directory you will find several files: one to record subscriptions information (`subs.dat`), and a series of files that logs the messages `msgs.1.dat`, etc...

The number of sub-directories, which again correspond to channels, can be limited by the configuration parameter `-max_channels`. When the limit is reached, any new subscription or message published on a new channel will produce an error.

On a given channel, the number of subscriptions can also be limited with the configuration parameter `-max_subs`. A client that tries to create a subscription on a given channel (subject) for which the limit is reached will receive an error.

Finally, the number of stored messages for a given channel can also be limited with the parameter `-max_msgs` and/or `-max_bytes`. However, for messages, the client does not get an error when the limit is reached. The oldest messages are discarded to make room for the new messages.

#### File Store Options

As described in the [Configuring](https://github.com/nats-io/nats-streaming-server#configuring) section, there are several options that you can use to configure a file store.

Regardless of channel limits, you can configure message logs to be split in individual files (called file slices). You can configure
those slices by number of messages it can contain (`--file_slice_max_msgs`), the size of the file - including the corresponding index file
(`--file_slice_max_bytes`), or the period of time that a file slice should cover - starting at the time the first message is stored in
that slice (`--file_slice_max_age`). The default file store options are defined such that only the slice size is configured to 64MB.

Note: If you don't configure any slice limit but you do configure channel limits, then the server will automatically
set some limits for file slices.

When messages accumulate in a channel, and limits are reached, older messages are removed. When the first file slice
becomes empty, the server removes this file slice (and corresponding index file).

However, if you specify a script (`--file_slice_archive_script`), then the server will rename the slice files (data and index)
with a `.bak` extension and invoke the script with the channel name, data and index file names.<br>
The files are left in the channel's directory and therefore it is the script responsibility to delete those files when done.
At any rate, those files will not be recovered on a server restart, but having lots of unused files in the directory may slow
down the server restart.

For instance, suppose the server is about to delete file slice `datastore/foo/msgs.1.dat` (and `datastore/foo/msgs.1.idx`),
and you have configured the script `/home/nats-streaming/archive_script.sh`. The server will invoke:

``` bash
/home/nats-streaming/archive_script.sh foo datastore/foo/msgs.1.dat.bak datastore/foo/msgs.2.idx.bak
```
Notice how the files have been renamed with the `.bak` extension so that they are not going to be recovered if
the script leave those files in place.


### Store Interface

Every store implementation follows the [Store interface](https://github.com/nats-io/nats-streaming-server/blob/master/stores/store.go).

The main interace is the `Store` which the server will create a unique instance of. From a store, the server can add/delete clients, create/lookup channels, etc...

Creating/looking up a channel will return a `ChannelStore`, which points to two other interfaces, the `SubStore` and `MsgStore`. These stores handle, for a given channel, subscriptions and messages respectiverly.

If you wish to contribute to a new store type, your implementation must include all these interfaces. For stores that allow recovery (such as file store as opposed to memory store), there are additional structures that have been defined and that a store constructor should return. This allows the server to reconstruct its state on startup.

The memory and the provided file store implementations both use a generic store implementation to avoid code duplication.
When writing your own store implementation, you can do the same for APIs that don't need to do more than what the generic implementation provides.
You can check [MemStore](https://github.com/nats-io/nats-streaming-server/blob/master/stores/memstore.go) and [FileStore](https://github.com/nats-io/nats-streaming-server/blob/master/stores/filestore.go) implementations for more details.

## Building

Building the NATS Streaming Server from source requires at least version 1.6 of Go, but we encourage the use of the latest stable release. Information on installation, including pre-built binaries, is available at http://golang.org/doc/install. Stable branches of operating system packagers provided by your OS vendor may not be sufficient.

Run `go version` to see the version of Go which you have installed.

Run `go build` inside the directory to build.

Run `go test ./...` to run the unit regression tests.

A successful build produces no messages and creates an executable called `nats-streaming-server` in the current directory. You can invoke that binary, with no options and no configuration file, to start a server with acceptable standalone defaults (no authentication, memory store).

Run go help for more guidance, and visit http://golang.org/ for tutorials, presentations, references and more.

## Clients

Here is the list of NATS Streaming clients, supported by Apcera. We may add additional supported streaming clients in the future, and encourage community-contributed clients.

- [C#](https://github.com/nats-io/csharp-nats-streaming)
- [Go](https://github.com/nats-io/go-nats-streaming)
- [Java](https://github.com/nats-io/java-nats-streaming)
- [Node.js](https://github.com/nats-io/node-nats-streaming)

## License

The MIT License (MIT)

Copyright (c) 2016 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


[License-Url]: http://opensource.org/licenses/MIT
[License-Image]: https://img.shields.io/npm/l/express.svg
[Build-Status-Url]: http://travis-ci.org/nats-io/nats-streaming-server
[Build-Status-Image]: https://travis-ci.org/nats-io/nats-streaming-server.svg?branch=master
[Coverage-Url]: https://coveralls.io/r/nats-io/nats-streaming-server?branch=master
[Coverage-image]: https://coveralls.io/repos/github/nats-io/nats-streaming-server/badge.svg?branch=master&t=kIxrDE
[ReportCard-Url]: http://goreportcard.com/report/nats-io/nats-streaming-server
[ReportCard-Image]: http://goreportcard.com/badge/github.com/nats-io/nats-streaming-server
[github-release]: https://github.com/nats-io/nats-streaming-server/releases/

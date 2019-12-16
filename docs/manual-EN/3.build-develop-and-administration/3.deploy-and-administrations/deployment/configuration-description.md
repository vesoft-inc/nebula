# Configuration Description

This documentation details the descriptions of the configuration files under the `etc/` directory.

## Meta Service

Property Name               | Default Value            | Description
--------------------------- | ------------------------ | -----------
`port`                      | 45500                    | Meta daemon listening port.
`reuse_port`                | true                     | Whether to turn on the SO_REUSEPORT option.
`data_path`                 | ""                       | Root data path. Multi-paths are not supported.
`meta_server_addrs`         | ""                       | A list of IPs split by comma, used in cluster deployment, the number of IPs is equal to that of the replica. If empty, it means a single node.
`local_ip`                  | ""                       | Local ip specified for NetworkUtils::getLocalIP.
`num_io_threads`            | 16                       | Number of IO threads.
`meta_http_thread_num`      | 3                        | Number of meta daemon's http thread.
`num_worker_threads`        | 32                       | Number of workers.
`part_man_type`             | memory                   | Memory, meta.
`pid_file`                  | "pids/nebula-metad.pid"  | File to hold the process ID.
`daemonize`                 | true                     | Whether run as a daemon process.
`cluster_id`                | 0                        | A unique id for each cluster.
`meta_ingest_thread_num`    | 3                        | Meta daemon's ingest thread number.
`ws_http_port`              | 11000                    | Port to listen on Meta with HTTP protocol are 11000.
`ws_h2_port`                | 11002                    | Port to listen on Meta with HTTP/2 protocol is 11002.
`ws_ip`                     | "127.0.0.1"              | IP/Hostname to bind to.
`ws_threads`                | 4                        | Number of threads for the web service.

## Storage Service

Property Name                       | Default Value              | Description
----------------------------------- | -------------------------- | -----------
`port`                              | 44500                      | Storage daemon listening port.
`reuse_port`                        | true                       | Whether to turn on the SO_REUSEPORT option.
`data_path`                         | ""                         | Root data path, multi paths should be split by comma. For RocksDB engine, one path one instance.
`local_ip`                          | ""                         | IP address used to identify this server, combined with the listen port.
`daemonize`                         | true                       | Whether to run the process as a daemon.
`pid_file`                          | "pids/nebula-storaged.pid" | File to hold the process ID.
`meta_server_addrs`                 | ""                         | List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3.
`store_type`                        | "nebula"                   | Which type of KVStore to be used by the storage daemon. Options can be nebula, HBase, etc.
`num_io_threads`                    | 16                         | Number of IO threads.
`storage_http_thread_num`           | 3                          | Number of storage daemon's http thread.
`num_worker_threads`                | 32                         | Number of workers.
`engine_type`                       | rocksdb                    | RocksDB, memory...
`custom_filter_interval_secs`       | 24 * 3600                  | Interval to trigger custom compaction.
`num_workers`                       | 4                          | Number of worker threads.
`rocksdb_disable_wal`               | false                      | Whether to disable the WAL in RocksDB.
`rocksdb_db_options`                | "{}"                       | Json string of DBOptions, all keys and values are string.
`rocksdb_column_family_options`     | "{}"                       | Json string of ColumnFamilyOptions, all keys and values are string.
`rocksdb_block_based_table_options` | "{}"                       | Json string of BlockBasedTableOptions, all keys and values are string.
`rocksdb_batch_size`                | 4 * 1024                   | Default reserved bytes for one batch operation.
`rocksdb_block_cache`               | 1024                       | The default block cache size used in BlockBasedTable. The unit is MB.
`download_thread_num`               | 3                          | Download thread number.
`min_vertices_per_bucket`           | 3                          | The min vertices number in one bucket.
`max_appendlog_batch_size`          | 128                        | The max number of logs in each appendLog request batch.
`max_outstanding_requests`          | 1024                       | The max number of outstanding appendLog requests.
`raft_rpc_timeout_ms`               | 500                        | RPC timeout for raft client.
`raft_heartbeat_interval_secs`      | 5                          | Seconds between each heartbeat.
`max_batch_size`                    | 256                        | The max number of logs in a batch.
`ws_http_port`                      | 12000                      | Port to listen on Storage with HTTP protocol is 12000.
`ws_h2_port`                        | 12002                      | Port to listen on Storage with HTTP/2 protocol is 12002.
`ws_ip`                             | "127.0.0.1"                | IP/Hostname to bind to.
`ws_threads`                        | 4                          | Number of threads for the web service.

## Graph Service

Property Name                   | Default Value            | Description
------------------------------- | ------------------------ | -----------
`port`                          | 3699                     | Nebula Graph daemon's listen port.
`client_idle_timeout_secs`      | 0                        | Seconds before we close the idle connections, 0 for infinite.
`session_idle_timeout_secs`     | 600                      | Seconds before we expire the idle sessions, 0 for infinite.
`session_reclaim_interval_secs` | 10                       | Period we try to reclaim expired sessions.
`num_netio_threads`             | 0                        | Number of networking threads, 0 for number of physical CPU cores.
`num_accept_threads`            | 1                        | Number of threads to accept incoming connections.
`num_worker_threads`            | 0                        | Number of threads to execute user queries.
`reuse_port`                    | false                    | Whether to turn on the SO_REUSEPORT option.
`listen_backlog`                | 1024                     | Backlog of the listen socket.
`listen_netdev`                 | "any"                    | The network device to listen on.
`pid_file`                      | "pids/nebula-graphd.pid" | File to hold the process ID.
`redirect_stdout`               | true                     | Whether to redirect stdout and stderr to separate files.
`stdout_log_file`               | "graphd-stdout.log"      | Destination filename of stdout.
`stderr_log_file`               | "graphd-stderr.log"      | Destination filename of stderr.
`daemonize`                     | true                     | Whether run as a daemon process.
`meta_server_addrs`             | ""                       | List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3.
`ws_http_port`                  | 13000                    | Port to listen on Graph with HTTP protocol is 13000.
`ws_h2_port`                    | 13002                    | Port to listen on Graph with HTTP/2 protocol is 13002.
`ws_ip`                         | "127.0.0.1"              | IP/Hostname to bind to.
`ws_threads`                    | 4                        | Number of threads for the web service.

## Console

Property Name            | Default Value | Description
------------------------ | ------------- | -----------
`addr`                   | "127.0.0.1"   | Graph daemon IP address.
`port`                   | 0             | Graph daemon listening port.
`u`                      | ""            | Username used to authenticate.
`p`                      | ""            | Password used to authenticate.
`enable_history`         | false         | Whether to force saving the command history.
`server_conn_timeout_ms` | 1000          | Connection timeout in milliseconds.

---

This tutorial provides an introduction to deploy `Nebula` cluster.

---

#### Download and install package

First at all, you can download rpm or deb from [Here]().

For `CentOS`:

```
rpm -ivh nebula-{VERSION}-0.el7.centos.x86_64.rpm
```

For `Ubuntu`:

```
dpkg -i nebula-{VERSION}-0.el7.centos.x86_64.deb
```

By default, the config files are under `/usr/local/etc`, you should modify the `meta_server_addrs` to set the Meta Server's address. 

In order to enable multi copy Meta service, you should set the meta addresses split by comma into `meta_server_addrs`.

Using `data_path` to set `Meta` and `Storage`'s underlying storage directory.

***

#### StartUp Nebula Cluster

Currently, we have support `scripts/services.sh` to management the nebula cluster.

You can `start`, `stop` and `restart` the cluster using this script.

It's looks like the following command:

```
scripts/services.sh <start|stop|restart|status|kill>
```

The metas, storages and graphs contain the host of them.

***

#### Config reference

**Meta Service** support the following config properties.

Property Name               | Default Value            | Description
--------------------------- | ------------------------ | -----------
`port`                      | 45500                    | Meta daemon listening port.
`reuse_port`                | true                     | Whether to turn on the SO_REUSEPORT option.
`data_path`                 | ""                       | Root data path.
`peers`                     | ""                       | It is a list of IPs split by comma, the ips number equals replica number. If empty, it means replica is 1.
`local_ip`                  | ""                       | Local ip speicified for NetworkUtils::getLocalIP.
`num_io_threads`            | 16                       | Number of IO threads.
`meta_http_thread_num`      | 3                        | Number of meta daemon's http thread.
`num_worker_threads`        | 32                       | Number of workers.
`part_man_type`             | memory                   | memory, meta.
`pid_file`                  | "pids/nebula-metad.pid"  | File to hold the process id.
`daemonize`                 | true                     | Whether run as a daemon process.
`cluster_id`                | 0                        | A unique id for each cluster.
`putTryNum`                 | 10                       | Try num of store clusterId to kvstore.
`load_config_interval_secs` | 2 * 60                   | Load config interval.
`meta_ingest_thread_num`    | 3.                       | Meta daemon's ingest thread number.

**Storage Service** support the following config properties.

Property Name                       | Default Value              | Description
----------------------------------- | -------------------------- | -----------
`port`                              | 44500                      | Storage daemon listening port.
`reuse_port`                        | true                       | Whether to turn on the SO_REUSEPORT option.
`data_path`                         | ""                         | Root data path, multi paths should be split by comma. For rocksdb engine, one path one instance.
`local_ip`                          | ""                         | IP address which is used to identify this server, combined with the listen port.
`mock_server`                       | true                       | Start mock server.
`daemonize`                         | true                       | Whether to run the process as a daemon.
`pid_file`                          | "pids/nebula-storaged.pid" | File to hold the process id.
`meta_server_addrs`                 | ""                         | List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3.
`store_type`                        | "nebula"                   | Which type of KVStore to be used by the storage daemon.Options can be \"nebula\", \"hbase\", etc.
`num_io_threads`                    | 16                         | Number of IO threads
`storage_http_thread_num`           | 3                          | Number of storage daemon's http thread.
`num_worker_threads`                | 32                         | Number of workers.
`engine_type`                       | rocksdb                    | rocksdb, memory...
`custom_filter_interval_secs`       | 24 * 3600                  | interval to trigger custom compaction.
`num_workers`                       | 4                          | Number of worker threads.
`rocksdb_disable_wal`               | false                      | Whether to disable the WAL in rocksdb.
`rocksdb_db_options`                | ""                         | DBOptions, each option will be given as <option_name>:<option_value> separated by.
`rocksdb_column_family_options`     | ""                         | ColumnFamilyOptions, each option will be given as <option_name>:<option_value> separated by.
`rocksdb_block_based_table_options` | ""                         | BlockBasedTableOptions, each option will be given as <option_name>:<option_value> separated by.
`batch_reserved_bytes`              | 4 * 1024                   | Default reserved bytes for one batch operation
`block_cache`                       | 4                          | BlockBasedTable:block_cache : MB
`download_thread_num`               | 3                          | Download thread number.
`min_vertices_per_bucket`           | 3                          | The min vertices number in one bucket.
`max_appendlog_batch_size`          | 128                        | The max number of logs in each appendLog request batch.
`max_outstanding_requests`          | 1024                       | The max number of outstanding appendLog requests.
`raft_rpc_timeout_ms`               | 500                        | RPC timeout for raft client.
`accept_log_append_during_pulling`  | false                      | Whether to accept new logs during pulling the snapshot.
`heartbeat_interval`                | 5                          | Seconds between each heartbeat.
`max_batch_size`                    | 256                        | The max number of logs in a batch.

**Graph Service** support the following config properties.

Property Name                   | Default Value            | Description
------------------------------- | ------------------------ | -----------
`port`                          | 34500                    | Nebula Graph daemon's listen port.
`client_idle_timeout_secs`      | 0                        | Seconds before we close the idle connections, 0 for infinite.
`session_idle_timeout_secs`     | 600                      | Seconds before we expire the idle sessions, 0 for infinite.
`session_reclaim_interval_secs` | 10                       | Period we try to reclaim expired sessions.
`num_netio_threads`             | 0                        | Number of networking threads, 0 for number of physical CPU cores.
`num_accept_threads`            | 1                        | Number of threads to accept incoming connections.
`num_worker_threads`            | 1                        | Number of threads to execute user queries.
`reuse_port`                    | true                     | Whether to turn on the SO_REUSEPORT option.
`listen_backlog`                | 1024                     | Backlog of the listen socket.
`listen_netdev`                 | "any"                    | The network device to listen on.
`pid_file`                      | "pids/nebula-graphd.pid" | File to hold the process id.
`redirect_stdout`               | true                     | Whether to redirect stdout and stderr to separate files.
`stdout_log_file`               | "graphd-stdout.log"      | Destination filename of stdout.
`stderr_log_file`               | "graphd-stderr.log"      | Destination filename of stderr.
`daemonize`                     | true                     | Whether run as a daemon process.
`meta_server_addrs`             | ""                       | List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3.



**Web Service** support the following config properties.

Property Name          | Default Value | Description
---------------------- | ------------- | -----------
`ws_http_port`         | 11000         | Port to listen on with HTTP protocol.
`ws_h2_port`           | 11002         | Port to listen on with HTTP/2 protocol.
`ws_ip`                | "127.0.0.1"   | IP/Hostname to bind to.
`ws_threads`           | 4             | Number of threads for the web service.
`ws_meta_http_port`    | 11000         | Port to listen on Meta with HTTP protocol.
`ws_meta_h2_port`      | 11002         | Port to listen on Meta with HTTP/2 protocol.
`ws_storage_http_port` | 12000         | Port to listen on Storage with HTTP protocol.
`ws_storage_h2_port`   | 12002         | Port to listen on Storage with HTTP/2 protocol.

**Console** support the following config properties.

Property Name            | Default Value | Description
------------------------ | ------------- | -----------
`addr`                   | "127.0.0.1"   | Nebula daemon IP address
`port`                   | 0             | Nebula daemon listening port.
`u`                      | ""            | Username used to authenticate.
`p`                      | ""            | Password used to authenticate.
`enable_history`         | false         | Whether to force saving the command history.
`server_conn_timeout_ms` | 1000          | Connection timeout in milliseconds.



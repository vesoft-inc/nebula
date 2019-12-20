# 配置文件说明

本文介绍 `etc/` 目录下配置文件所对应参数。

## Meta Service

属性名               | 默认值            | 说明
--------------------------- | ------------------------ | -----------
`port`                      | 45500                    | Meta daemon 监听端口
`reuse_port`                | true                     | 开启 SO_REUSEPORT 选项
`data_path`                 | ""                       | 根数据路径，不支持多条路径
`meta_server_addrs`         | ""                       | 一系列由逗号分隔的 IP 地址，用于集群部署，IP 数与副本数相等，如果为空，则表明是单机
`local_ip`                  | ""                       | 为 NetworkUtils :: getLocalIP 指定本地 IP  
`num_io_threads`            | 16                       | IO 线程数
`meta_http_thread_num`      | 3                        | meta daemon 的 http 线程数
`num_worker_threads`        | 32                       | worker 数
`part_man_type`             | memory                   | memory，meta
`pid_file`                  | "pids/nebula-metad.pid"  | 存储进程 ID 的文件
`daemonize`                 | true                     | 作为 daemon 进程运行
`cluster_id`                | 0                        | 集群的唯一 ID
`meta_ingest_thread_num`    | 3                        | Meta daemon 的 ingest 线程数
`ws_http_port`         | 11000         |  Meta HTTP 协议监听端口默认值为 11000
`ws_h2_port`           | 11002        |  Meta HTTP/2 协议监听端口默认值为 11002
`ws_ip`                | "127.0.0.1"   | IP/Hostname 绑定地址
`ws_threads`           | 4             | web service 线程数

## Storage Service

属性名                       | 默认值              | 说明
----------------------------------- | -------------------------- | -----------
`port`                              | 44500                      | Storage daemon 监听端口
`reuse_port`                        | true                       | 开启 SO_REUSEPORT 选项
`data_path`                         | ""                         | 根数据路径，多条路径由逗号分隔，对于 RocksDB 引擎，一个路径为一个实例
`local_ip`                          | ""                         |  IP 地址和监听端口共同用于标识此服务器
`daemonize`                         | true                       | 作为 daemon 进程运行
`pid_file`                          | "pids/nebula-storaged.pid" | 存储进程 ID 的文件
`meta_server_addrs`                 | ""                         | meta server 地址列表，格式为 ip1:port1, ip2:port2, ip3:port3
`store_type`                        | "nebula"                   | storage daemon 使用的 KVStore 类型，可选类型为 nebula、HBase 等
`num_io_threads`                    | 16                         | IO 线程数
`storage_http_thread_num`           | 3                          | storage daemon 的 http 线程数
`num_worker_threads`                | 32                         | worker 数
`engine_type`                       | rocksdb                    | RocksDB, memory ...
`custom_filter_interval_secs`       | 24 * 3600                  | 触发自定义压缩间隔
`num_workers`                       | 4                          | worker 线程数
`rocksdb_disable_wal`               | false                      |  禁用 RocksDB 的 WAL
`rocksdb_db_options`                | "{}"                         |  DBOptions 的 Json 字符串，所有键值均为字符串
`rocksdb_column_family_options`     | "{}"                         | ColumnFamilyOptions 的 Json 字符串，所有键值均为字符串
`rocksdb_block_based_table_options` | "{}"                         | BlockBasedTableOptions 的 Json 字符串，所有键值均为字符串
`rocksdb_batch_size`                        | 4 * 1024                   | 单个批处理的默认保留字节
`rocksdb_block_cache`                       | 4                          | BlockBasedTable 使用的默认块缓存大小。单位是 MB。
`download_thread_num`               | 3                          | 下载线程数
`min_vertices_per_bucket`           | 3                          | 一个 bucket 中最小节点数
`max_appendlog_batch_size`          | 128                        | 每个 appendLog 批请求的最大 log 数
`max_outstanding_requests`          | 1024                       | outstanding appendLog 请求最大数
`raft_rpc_timeout_ms`               | 500                        | raft 客户端 RPC 超时时长，单位毫秒
`raft_heartbeat_interval_secs`      | 5                          | 每次心跳间隔时长，单位秒
`max_batch_size`                    | 256                        | 一个 batch 中最大 log 数
`ws_http_port`         | 12000         |  Storage HTTP 12000
`ws_h2_port`           | 12002         |  Storage HTTP/2 协议监听端口默认值为 12002
`ws_ip`                | "127.0.0.1"   | IP/Hostname 绑定地址
`ws_threads`           | 4             | web service 线程数

## Graph Service

属性名                   | 默认值            | 说明
------------------------------- | ------------------------ | -----------
`port`                          | 3699                     | Nebula Graph daemon 监听端口
`client_idle_timeout_secs`      | 0                        | 关闭 idle 连接前的时长（单位秒）， 0 为无穷大
`session_idle_timeout_secs`     | 600                      | idle sessions 过期时长（单位秒），0 为无穷大
`session_reclaim_interval_secs` | 10                       | 超出指定时间则认为超时
`num_netio_threads`             | 0                        | networking 线程数，0为物理 CPU 核数
`num_accept_threads`            | 1                        | 接受进入连接的线程数
`num_worker_threads`            | 0                        | 执行用户请求的线程数，线程数为系统 CPU 核数
`reuse_port`                    | false                     | 开启 SO_REUSEPORT 选项
`listen_backlog`                | 1024                     | listen socket 的 backlog
`listen_netdev`                 | "any"                    | 监听的网络服务
`pid_file`                      | "pids/nebula-graphd.pid" | 存储进程 ID 的文件
`redirect_stdout`               | true                     | 将 stdout 和 stderr 重定向到单独的文件
`stdout_log_file`               | "graphd-stdout.log"      | stdout 目标文件名
`stderr_log_file`               | "graphd-stderr.log"      | stderr 目标文件名
`daemonize`                     | true                     | 作为 daemon 进程运行
`meta_server_addrs`             | ""                       |  meta server 地址列表，格式为 ip1:port1, ip2:port2, ip3:port3
`ws_http_port`         | 13000         |  Graph HTTP 协议监听端口默认值为 13000
`ws_h2_port`           | 13002        |  Graph HTTP/2 协议监听端口默认值为 13002
`ws_ip`                | "127.0.0.1"   | IP/Hostname 绑定地址
`ws_threads`           | 4             | web service 线程数

## Console

属性名            | 默认值 | 说明
------------------------ | ------------- | -----------
`addr`                   | "127.0.0.1"   | Graph daemon IP 地址
`port`                   | 0             | Graph daemon 监听端口
`u`                      | ""            | 用于身份验证的用户名
`p`                      | ""            | 用于身份验证的密码
`enable_history`         | false         | 是否保存历史命令
`server_conn_timeout_ms` | 1000          | 连接超时时长，单位毫秒

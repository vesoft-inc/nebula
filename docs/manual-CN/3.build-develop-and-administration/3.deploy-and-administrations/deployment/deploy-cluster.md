---

本节介绍`Nebula`的部署
---

#### 下载并安装包

目前，Nebula官方提供 `CentOS 7.5`， `CentOS 6.5`，`Ubuntu 1604`和`Ubuntu 1804`包，rpm或deb包下载点击[此处](https://github.com/vesoft-inc/nebula/releases)。

 `CentOS`系统：

```
rpm -ivh nebula-{VERSION}.{SYSTEM_VERSION}.x86_64.rpm
```

 `Ubuntu`系统：

```
dpkg -i nebula-{VERSION}.{SYSTEM_VERSION}.amd64.deb
```

配置文件默认目录为`/usr/local/nebula/etc`，请更改`meta_server_addrs`，配置 Meta Server 的地址。

启动多副本 Meta 服务需将多个地址配置到`meta_server_addrs`，地址间需使用逗号分隔。

使用`data_path`设置`Meta`的底层存储路径。
***

#### 启动 Nebula 集群


目前，nebula 集群由`scripts/services.sh`运维，可使用此脚本`start`，`stop`或 `restart`重启集群。

示例命令如下：

```
scripts/services.sh <start|stop|restart|status|kill>
```

metas， storages 和 graphs 包含其自身的 hosts。

***

#### 配置引用

**Meta Service** 支持如下配置属性：

属性名               | 默认值            | 说明
--------------------------- | ------------------------ | -----------
`port`                      | 45500                    | Meta daemon 监听端口
`reuse_port`                | true                     | 开启 SO_REUSEPORT 选项
`data_path`                 | ""                       | 根数据目录，不支持多目录
`meta_server_addrs`                     | ""                       | 一系列由逗号分隔的IP地址，IP数与副本数相等，如果为空，则表明副本数为1
`local_ip`                  | ""                       | 指定本地IP  NetworkUtils::getLocalIP.
`num_io_threads`            | 16                       | IO 线程数
`meta_http_thread_num`      | 3                        | meta daemon 的 http 线程数
`num_worker_threads`        | 32                       | worker 数
`part_man_type`             | memory                   | memory，meta
`pid_file`                  | "pids/nebula-metad.pid"  | 存储进程 ID 的文件
`daemonize`                 | true                     | 作为 daemon 进程运行
`load_config_interval_secs` | 2 * 60                   | 加载配置间隔
`meta_ingest_thread_num`    | 3.                       | Meta daemon的 ingest 线程数

**Storage Service** 支持如下配置属性：

属性名                       | 默认值              | 说明
----------------------------------- | -------------------------- | -----------
`port`                              | 44500                      | Storage daemon 监听端口
`reuse_port`                        | true                       | 开启 SO_REUSEPORT 选项
`data_path`                         | ""                         | 根数据目录，多条路径由逗号分隔，对 rocksdb 引擎，一个路径为一个实例
`local_ip`                          | ""                         |  IP 地址和监听端口共同用于标识此服务器
`daemonize`                         | true                       | 作为 daemon 进程运行
`pid_file`                          | "pids/nebula-storaged.pid" | 存储进程 ID 的文件
`meta_server_addrs`                 | ""                         | meta server 地址列表，格式为 ip1:port1, ip2:port2, ip3:port3
`store_type`                        | "nebula"                   | storage daemon 使用的 KVStore 类型，可选类型为 \"nebula\"和\"hbase\"
`num_io_threads`                    | 16                         | IO 线程数
`storage_http_thread_num`           | 3                          | storage daemon 的 http 线程数
`num_worker_threads`                | 32                         | worker 数
`engine_type`                       | rocksdb                    | rocksdb, memory...
`custom_filter_interval_secs`       | 24 * 3600                  | 触发自定义压缩间隔
`num_workers`                       | 4                          | worker 线程数
`rocksdb_disable_wal`               | false                      |  禁用 rockdb 的 WAL
`rocksdb_db_options`                | ""                         |  DBOptions，每个选项以 <option_name>:<option_value> 格式给出，以.分隔
`rocksdb_column_family_options`     | ""                         | ColumnFamilyOptions，每个选项以 <option_name>:<option_value> 格式给出，以.分隔
`rocksdb_block_based_table_options` | ""                         | BlockBasedTableOptions，每个选项以<option_name>:<option_value> 格式给出，以.分隔
`batch_size`                        | 4 * 1024                   | 单个批处理的默认保留字节
`block_cache`                       | 4                          | BlockBasedTable:block_cache : MB
`download_thread_num`               | 3                          | 下载线程数
`min_vertices_per_bucket`           | 3                          | 一个bucket中最小节点数
`max_appendlog_batch_size`          | 128                        | 每个appendLog批请求的最大log数
`max_outstanding_requests`          | 1024                       | outstanding appendLog 请求最大数
`raft_rpc_timeout_ms`               | 500                        | raft 客户端 RPC 超时时长，单位毫秒
`accept_log_append_during_pulling`  | false                      |  pull snapshot 过程中不接受新log
`raft_heartbeat_interval_secs`      | 5                          | 每次心跳间隔时长，单位秒
`max_batch_size`                    | 256                        | 一个batch中最大log数

**Graph Service** 支持如下配置属性：

属性名                   | 默认值            | 说明
------------------------------- | ------------------------ | -----------
`port`                          | 3699                     | Nebula Graph daemon 监听端口
`client_idle_timeout_secs`      | 0                        | 关闭 idle 连接前的时长（单位秒）， 0为 无穷大
`session_idle_timeout_secs`     | 600                      | idle sessions过期时长（单位秒），0为无穷大
`session_reclaim_interval_secs` | 10                       | Period we try to reclaim expired sessions.
`num_netio_threads`             | 0                        | networking 线程数，0为物理 CPU 核数
`num_accept_threads`            | 1                        | 接受进入连接的线程数
`num_worker_threads`            | 1                        | 执行用户查询的线程数
`reuse_port`                    | true                     | 开启 SO_REUSEPORT 选项
`listen_backlog`                | 1024                     | listen socket 的 backlog
`listen_netdev`                 | "any"                    | 监听的网络服务
`pid_file`                      | "pids/nebula-graphd.pid" | 存储进程 ID 的文件
`redirect_stdout`               | true                     | 将 stdout 和 stderr 重定向到单独的文件
`stdout_log_file`               | "graphd-stdout.log"      | stdout 目标文件名
`stderr_log_file`               | "graphd-stderr.log"      | stderr 目标文件名
`daemonize`                     | true                     | 作为 daemon 进程运行
`meta_server_addrs`             | ""                       |  meta server 地址列表，格式为 ip1:port1， ip2:port2， ip3:port3.



**Web Service** 支持如下配置属性：

属性名         | 默认值 | 说明
---------------------- | ------------- | -----------
`ws_http_port`         | 11000         |  HTTP 协议监听端口
`ws_h2_port`           | 11002         |  HTTP/2 协议监听端口
`ws_ip`                | "127.0.0.1"   | IP/Hostname 绑定地址
`ws_threads`           | 4             | web service 线程数
`ws_meta_http_port`    | 11000         |  Meta HTTP 协议监听端口
`ws_meta_h2_port`      | 11002         |  Meta HTTP/2 协议监听端口
`ws_storage_http_port` | 12000         | Storage HTTP 协议监听端口
`ws_storage_h2_port`   | 12002         | Storage HTTP/2 协议监听端口

**Console** 支持如下配置属性：

属性名            | 默认值 | 说明
------------------------ | ------------- | -----------
`addr`                   | "127.0.0.1"   | Nebula daemon IP 地址
`port`                   | 0             | Nebula daemon 监听端口
`u`                      | ""            | 用于身份验证的用户名
`p`                      | ""            | 用于身份验证的密码
`enable_history`         | false         | 是否强制保存command历史
`server_conn_timeout_ms` | 1000          | 连接超时时长，单位毫秒

**注意：** 配置时请确保端口未被防火墙阻拦

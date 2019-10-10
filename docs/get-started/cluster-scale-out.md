# Cluster Scale Out

Cluster expansion allows the user to add new machine nodes to an existing cluster and to redistribute data. This document introduces how to horizontally scale out a nebula cluster. Horizontally scaling out nebula means scaling nebula out by adding nodes. When scaling out a cluster, the data is scaled out sequentially according to the values of replica value.

## Prerequisites

- The current nebula system must be installed in a cluster mode, not a single-node mode.
- The new nodes are available.

## Cluster Expansion Workflow

### Connect to Nebula

First check your IP address and configure **nebula-metad.conf** using the following command.

```
$ ip addr
$ cd /usr/local/nebula/etc
$ vi nebula-metad.conf

########## networking ##########
# Local ip
--local_ip={your local ip}
```

When configuration is done, try to connect to nebula using the following command.

```
$ scripts/nebula.service start all

[WARN] The maximum files allowed to open might be too few: 1024
[INFO] Starting nebula-metad...
[INFO] Done
[INFO] Starting nebula-graphd...
[INFO] Done
[INFO] Starting nebula-storaged...
[INFO] Done

$ scripts/nebula.service status all

[INFO] nebula-metad: Running as 21935, Listening on 45500
[INFO] nebula-graphd: Running as 22024, Listening on 3699
[INFO] nebula-storaged: Running as 22042, Listening on 44500
```

The above information indicates that nebula has been connected successfully. Some users may receive a warning `The maximum files allowed to open might be too few: 1024`, change the `/etc/security/limits.conf` file using the following command.

```
ulimit -v
```

## Add New Nodes to Cluster

Assuming the original cluster has 7 hosts with 3 partitions, 3 replicas. Let's scale the cluster to 9 hosts (one partition one host). In this document, we only scale out storage service, therefore only start multiple storages using the following command.

```
$ cd /usr/local/nebula/etc
$ cp nebula-storaged.conf nebula-storaged2.conf
$ cp nebula-storaged.conf nebula-storaged3.conf
$ cp nebula-storaged.conf nebula-storaged4.conf
$ cp nebula-storaged.conf nebula-storaged5.conf
$ cp nebula-storaged.conf nebula-storaged6.conf
$ cp nebula-storaged.conf nebula-storaged7.conf
$ cp nebula-storaged.conf nebula-storaged8.conf
$ cp nebula-storaged.conf nebula-storaged9.conf

```

Configure the storage files as follows, take **nebula-storaged2.conf** as example.

```
# The file to host the process id
--pid_file=pids/nebula-storaged2.pid
# Storage daemon listening port
--port=34510
# HTTP service port
--ws_http_port=12100
# HTTP2 service port
--ws_h2_port=12012
# One path per instance, if --engine_type is `rocksdb'
--data_path=data/storage2
```

**_Note_:** Make sure the ports are available when configuring with the command `netstat -anp|grep {port number}`.

When all the configurations are done, start all the storage services using the following command.

```
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged.conf
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged2.conf
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged3.conf
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged4.conf
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged5.conf
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged6.conf
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged7.conf
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged8.conf
$ ./bin/nebula-storaged --flagfile etc/nebula-storaged9.conf
```

**_Note_:** Services should be started under `nebula` directory.

Make sure the storage processes are available using command `ps -ef|grep storage
`.

## Write Data

Reconnect nebula and create a space named `test` using the following command.

```
$ scripts/nebula.service stop all
$ scripts/nebula.service status all
$ scripts/nebula.service start all
$ bin/nebula -u=user -p=password --addr={graphd IP address} --port={graphd listening port}
(user@127.0.0.1) [(none)]> create space test(partition_num=3,replica_factor=3)
Execution succeeded (Time spent: 2975/3618 us)
(user@127.0.0.1) [(none)]> show spaces
========
| Name |
========
| test |
--------
Got 1 rows (Time spent: 1039/1697 us)
```

**_Note_:** Refer [Get Started](https://github.com/vesoft-inc/nebula/blob/master/docs/get-started.md) for details on connecting nebula.

Run the follow command under `/usr/local/nebula/bin`.

```
./simple_kv_verify --meta_server_addrs={meta ip}:{meta port} --space_name=test
```

**_Note_:** Check your meta ip and port in `/usr/local/nebula/etc/nebula-metad.conf`.

## Redistribute Data

Show the partitions allocation with `SHOW HOSTS`.

```
(user@127.0.0.1) [(none)]> show hosts

============================================================================================
| Ip        | Port  | Status | Leader count | Leader distribution | Partition distribution |
============================================================================================
| 127.0.0.1 | 34560 | online | 0            | No valid partition  | No valid partition     |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34570 | online | 0            | test: 0             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34580 | online | 0            | test: 0             | test: 2                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34510 | online | 3            | test: 3             | test: 3                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 44500 | online | 0            | test: 0             | test: 2                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34520 | online | 0            | test: 0             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34530 | online | 0            | No valid partition  | No valid partition     |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34540 | online | 0            | No valid partition  | No valid partition     |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34550 | online | 0            | No valid partition  | No valid partition     |
--------------------------------------------------------------------------------------------
Got 9 rows (Time spent: 12287/13286 us)
```

Balance the data.

```
(user@127.0.0.1) [(none)]> balance data

==============
| ID         |
==============
| 1570689993 |
--------------
Got 1 rows (Time spent: 5766/6528 us)
```

The balance process has been run in the backend, check the detailed balance plan using `balance data $id`.

```
(user@127.0.0.1) [(none)]> balance data 1570689993

====================================================================
| balanceId, spaceId:partId, src->dst                  | status    |
====================================================================
| [1570689993, 1:1, 127.0.0.1:34580->127.0.0.1:34560]  | succeeded |
--------------------------------------------------------------------
| [1570689993, 1:1, 127.0.0.1:34510->127.0.0.1:34550]  | succeeded |
--------------------------------------------------------------------
| [1570689993, 1:2, 127.0.0.1:34510->127.0.0.1:34530]  | succeeded |
--------------------------------------------------------------------
| [1570689993, 1:2, 127.0.0.1:44500->127.0.0.1:34540]  | succeeded |
--------------------------------------------------------------------
Got 4 rows (Time spent: 1452/2312 us)
```

When balance is done, check the status with `SHOW HOSTS`.

```
(user@127.0.0.1) [test]> show hosts

============================================================================================
| Ip        | Port  | Status | Leader count | Leader distribution | Partition distribution |
============================================================================================
| 127.0.0.1 | 34560 | online | 0            | test: 0             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34570 | online | 1            | test: 1             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34580 | online | 1            | test: 1             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34510 | online | 1            | test: 1             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 44500 | online | 0            | test: 0             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34520 | online | 0            | test: 0             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34530 | online | 0            | test: 0             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34540 | online | 0            | test: 0             | test: 1                |
--------------------------------------------------------------------------------------------
| 127.0.0.1 | 34550 | online | 0            | test: 0             | test: 1                |
--------------------------------------------------------------------------------------------
Got 9 rows (Time spent: 10270/11029 us)
```
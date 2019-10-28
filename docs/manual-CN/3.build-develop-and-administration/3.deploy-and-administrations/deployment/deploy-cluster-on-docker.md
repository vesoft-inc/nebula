# 使用 Docker 部署集群

本文介绍如何使用 Docker 部署一个多节点的 Nebula 集群。

**_注意_：** 由于 Nebula 的服务之间通信需要开放一些端口，所以可以临时关掉防火墙。对于生产环境，不要使用该方式进行部署。


## 环境准备

### 安装 Docker

Docker 安装方法请参考 [Docker 官方文档](https://docs.docker.com/)。


### 拉取镜像

从 Docker 镜像仓库获取最新 [Nebula Graph 镜像](https://hub.docker.com/r/vesoft/nebula-graph)

```bash
$ docker pull vesoft/nebula-graph:nightly

Pulling from vesoft/nebula-graph
d8d02d457314: Pull complete
f7022daf2b4f: Pull complete
106b632a856a: Pull complete
Digest: sha256:313214ca1a4482183a0352450639d6dd79d77c56143654c57674c06131d00a47
Status: Downloaded newer image for vesoft/nebula-graph:nightly
```


## 部署一个多节点集群


### 查看每个容器的IP

镜像拉取完成后，使用以下命令打开三个 docker 容器

```bash
$ docker run -it vesoft/nebula-graph:nightly /bin/bash
```

查看 docker 进程

```bash
$ docker ps

CONTAINER ID        IMAGE                         COMMAND             CREATED             STATUS              PORTS               NAMES
c2134fd5ccc3        vesoft/nebula-graph:nightly   "/bin/bash"         5 minutes ago       Up 5 minutes                            thirsty_grothendieck
1d7a441d4f40        vesoft/nebula-graph:nightly   "/bin/bash"         5 minutes ago       Up 5 minutes                            elastic_feistel
591e2f6f48e2        vesoft/nebula-graph:nightly   "/bin/bash"         7 minutes ago       Up 7 minutes                            sad_chaum
```

使用以下命令查看每个 docker 进程的 IP

```bash
$ docker inspect 容器ID | grep IPAddress
```

```bash
$ docker inspect c2134fd5ccc3 | grep IPAddress

            "SecondaryIPAddresses": null,
            "IPAddress": "172.17.0.4",
                    "IPAddress": "172.17.0.4",
$ docker inspect 1d7a441d4f40 | grep IPAddress

            "SecondaryIPAddresses": null,
            "IPAddress": "172.17.0.3",
                    "IPAddress": "172.17.0.3",
$ docker inspect 591e2f6f48e2 | grep IPAddress

            "SecondaryIPAddresses": null,
            "IPAddress": "172.17.0.2",
                    "IPAddress": "172.17.0.2",
```


因此本文将在3台主机上按如下的方式部署 Nebula 的集群

```
172.17.0.2 # cluster-2: metad/storaged/graphd
172.17.0.3 # cluster-3: metad/storaged/graphd
172.17.0.4 # cluster-4: metad/storaged/graphd
```

**_注意_：** 由于 Nebula 的服务之间通信需要开放一些端口，所以可以临时关掉所有机器上的防火墙，具体使用端口见 `/usr/local/nebula/etc/ `下面的配置文件。也可根据实际情况灵活选取部署方式，此处仅做测试用。

## 配置

Nebula 的所有配置文件都位于 `/usr/local/nebula/etc` 目录下，并且提供了三份默认配置。分别编辑这些配置文件:

第一份配置文件 **nebula-metad.conf**

metad 通过 raft 协议保证高可用，需要为每个 metad 的 service 都配置该服务部署的机器 ip 和端口。主要涉及 meta_server_addrs 和 local_ip 两个字段，其他使用默认配置。cluster-2 上的两项配置示例如下所示

```
# Peers
--meta_server_addrs=172.17.0.2:45500,172.17.0.3:45500,172.17.0.4:45500
# Local ip
--local_ip=172.17.0.2
# Meta daemon listening port
--port=45500
```

![image](https://user-images.githubusercontent.com/42762957/66463034-dcbb9200-eaae-11e9-896b-c823318cb58e.png)

第二份配置文件 **nebula-graphd.conf**

graphd 运行时需要从 metad 中获取 Schema 数据，所以在配置中必须显示指定集群中 metad 的 ip 地址和端口选项 meta_server_addrs ，其他使用默认配置。cluster-2 上的 graphd 配置如下

```
# Meta Server Address
--meta_server_addrs=172.17.0.2:45500,172.17.0.3:45500,172.17.0.4:45500
```

![image](https://user-images.githubusercontent.com/42762957/66463601-fb6e5880-eaaf-11e9-8067-1c7a8b2a52b0.png)


第三份配置文件 **nebula-storaged.conf**

storaged 也使用 raft 协议保证高可用，在数据迁移时会与 metad 通信，所以需要配置 metad 的地址和端口 `meta_server_addrs` 和本机地址 `local_ip` ，其 peers 可以通过 metad 获得。cluster-2 上的部分配置选项如下

```
# Meta server address
--meta_server_addrs=172.17.0.2:45500,172.17.0.3:45500,172.17.0.4:45500
# Local ip
--local_ip=172.17.0.2
# Storage daemon listening port
--port=44500
```

![image](https://user-images.githubusercontent.com/42762957/66463419-99adee80-eaaf-11e9-921f-c5648093d6c9.png)


请重复上述步骤，为 cluster-3、cluster-4 进行配置，一共需配置9个文件。


## 启动集群

集群配置完成后需重启服务

```bash
$ /usr/local/nebula/scripts/nebula.service stop all

[INFO] Stopping nebula-metad...
[INFO] Done
[INFO] Stopping nebula-graphd...
[INFO] Done
[INFO] Stopping nebula-storaged...
[INFO] Done

$ /usr/local/nebula/scripts/nebula.service start all

[INFO] Starting nebula-metad...
[INFO] Done
[INFO] Starting nebula-graphd...
[INFO] Done
[INFO] Starting nebula-storaged...
[INFO] Done
```

重复以上命令重启 cluster-3、cluster-4。


## 测试集群

使用客户端登录集群中的一台，执行如下命令

```bash
$ /usr/local/nebula/bin/nebula -u user -p password --addr 172.17.0.2 --port 3699

Welcome to Nebula Graph (Version 5f656b5)

(user@172.17.0.2) [(none)]> show hosts

=============================================================================================
| Ip         | Port  | Status | Leader count | Leader distribution | Partition distribution |
=============================================================================================
| 127.17.0.2 | 44500 | online | 1            | space 1: 1          | space 1: 1             |
---------------------------------------------------------------------------------------------
| 172.17.0.3 | 44500 | online | 1            | space 1: 1          | space 1: 1             |
---------------------------------------------------------------------------------------------
| 172.17.0.4 | 44500 | online | 0            |                     | space 1: 1             |
---------------------------------------------------------------------------------------------
```

表明三台集群已部署成功，插入[数据](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-CN/get-started.md#%E5%88%9B%E5%BB%BA%E5%9B%BE%E6%95%B0%E6%8D%AE)进行测试。

```
$a=GO FROM 201 OVER like yield like._dst as id; GO FROM $a.id OVER select YIELD $^.student.name AS Student, $$.course.name AS Course, select.grade AS Grade

=============================
| Student | Course  | Grade |
=============================
| Monica  | Math    | 5     |
-----------------------------
| Monica  | English | 3     |
-----------------------------
| Jane    | English | 3     |
-----------------------------
```

停止 cluster-4 的 storage 进程

```bash
$ /usr/local/nebula/scripts/nebula.service stop storaged
```

查看进程状态，确认进程是否停止

```bash
$ /usr/local/nebula/scripts/nebula.service status storaged
[INFO] nebula-storaged: Exited
```

登录 cluster-2，使用 `SHOW HOSTS` 查看

```
> SHOW HOSTS

=============================================================================================
| Ip         | Port  | Status | Leader count | Leader distribution | Partition distribution |
=============================================================================================
| 127.17.0.2 | 44500 | online | 1            | space 1: 1          | space 1: 1             |
---------------------------------------------------------------------------------------------
| 172.17.0.3 | 44500 | online | 1            | space 1: 1          | space 1: 1             |
---------------------------------------------------------------------------------------------
| 172.17.0.4 | 44500 | offline | 0            |                     | space 1: 1             |
---------------------------------------------------------------------------------------------
```

此时 cluster-4 状态显示为 offline，表明已成功停止。

**_注意：_** 如果 172.17.0.4 状态显示为 online，是因为 `expired_threshold_sec` 的默认值为10分钟。请自行修改配置文件 `nebula-metad.conf` 中的 `expired_threshold_sec` 参数值。也可更改配置文件 `nebula-storaged.conf` 中的 `heartbeat_interval_secs`。多副本情况下，只有在大多数副本可用时才能读取数据。

测试数据是否可读

```
$a=GO FROM 201 OVER like yield like._dst as id; GO FROM $a.id OVER select YIELD $^.student.name AS Student, $$.course.name AS Course, select.grade AS Grade

=============================
| Student | Course  | Grade |
=============================
| Monica  | Math    | 5     |
-----------------------------
| Monica  | English | 3     |
-----------------------------
| Jane    | English | 3     |
-----------------------------
```

数据可读，说明部署成功。


## 自定义配置文件

Nebula 支持通过指定配置文件的方式来加载更加丰富的启动参数，用于性能调优。详情请参考[配置属性](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-CN/deploy-cluster.md#%E9%85%8D%E7%BD%AE%E5%BC%95%E7%94%A8)。


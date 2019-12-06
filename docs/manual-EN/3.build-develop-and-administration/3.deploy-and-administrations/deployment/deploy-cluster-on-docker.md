# Deploy Clusters on Docker

Reference another repository: [vesoft-inc/nebula-docker-compose](https://github.com/vesoft-inc/nebula-docker-compose)

<!--
This article describes how to deploy a multi-node Nebula Graph cluster on Docker.

**_Note_:** This is for testing only. DO NOT USE in production.

## Preparation

### Install Docker

Before you start, make sure that you have installed the latest version of [Docker](https://docs.docker.com/).

### Pull Docker Image

Pull the latest image of Nebula Graph from [Docker Hub](https://hub.docker.com/r/vesoft/nebula-graph/tags) using the following command

```bash
$docker pull vesoft/nebula-graph:nightly

Pulling from vesoft/nebula-graph
d8d02d457314: Pull complete
f7022daf2b4f: Pull complete
106b632a856a: Pull complete
Digest: sha256:313214ca1a4482183a0352450639d6dd79d77c56143654c57674c06131d00a47
Status: Downloaded newer image for vesoft/nebula-graph:nightly
```

## Multi Nodes Deployment

### Check The IP of Each Container

After the image is pulled completely, start three containers using the following command

```bash
$docker run -it vesoft/nebula-graph:nightly /bin/bash
```

View their processes using the following command

```bash
$docker ps

CONTAINER ID        IMAGE                         COMMAND             CREATED             STATUS              PORTS               NAMES
c2134fd5ccc3        vesoft/nebula-graph:nightly   "/bin/bash"         5 minutes ago       Up 5 minutes                            thirsty_grothendieck
1d7a441d4f40        vesoft/nebula-graph:nightly   "/bin/bash"         5 minutes ago       Up 5 minutes                            elastic_feistel
591e2f6f48e2        vesoft/nebula-graph:nightly   "/bin/bash"         7 minutes ago       Up 7 minutes                            sad_chaum
```

Use the following command to check the IP address of each docker process:

```bash
$docker inspect {container ID} | grep IPAddress
```

```bash
$docker inspect c2134fd5ccc3 | grep IPAddress

            "SecondaryIPAddresses": null,
            "IPAddress": "172.17.0.4",
                    "IPAddress": "172.17.0.4",
$docker inspect 1d7a441d4f40 | grep IPAddress

            "SecondaryIPAddresses": null,
            "IPAddress": "172.17.0.3",
                    "IPAddress": "172.17.0.3",
$docker inspect 591e2f6f48e2 | grep IPAddress

            "SecondaryIPAddresses": null,
            "IPAddress": "172.17.0.2",
                    "IPAddress": "172.17.0.2",
```

Therefore, this article will deploy Nebula Graph cluster on three hosts as follows:

```plain
172.17.0.2 # cluster-2: metad/storaged/graphd
172.17.0.3 # cluster-3: metad/storaged/graphd
172.17.0.4 # cluster-4: metad/storaged/graphd
```

**_Note_:** Please turn off the firewall on all the nodes temporarily as some ports are needed in nebula services communications. Refer to config file in `/usr/local/nebula/etc/` to check the in use ports. In production, please choose deployment method based on your actual conditions. This is for testing only.

## Configuration

All the configuration files of Nebula Graph are located in `/usr/local/nebula/etc`, and three default configuration files are provided there. Edit them separately:

First configuration file: **nebula-metad.conf**

Metad ensures high availability through the raft protocol. So you need to configure the ip and port deployed for each metad service. You need to configure `meta_server_addrs` and `local_ip` and leave other parameters with the default values. Consider cluster-2 configuration as example:

```plain
# Peers
--meta_server_addrs=172.17.0.2:45500,172.17.0.3:45500,172.17.0.4:45500
# Local ip
--local_ip=172.17.0.2
# Meta daemon listening port
--port=45500
```

Second configuration file: **Nebula-graphd.conf**

Graphd needs to obtain the Schema data from metad when running, so metad's IP address and port `meta_server_addrs` must be specified in configuration, other parameters can use the default values. Consider cluster-2 configuration as example:

```plain
# Meta Server Address
--meta_server_addrs=172.17.0.2:45500,172.17.0.3:45500,172.17.0.4:45500
```

Third configuration file: **Nebula-storaged.conf**

Storaged also uses the raft protocol to ensure high availability and communicates with metad during data migration. Therefore, you need to configure the address and port `meta_server_addrs` of metad and the `local_ip`, which can be obtained by its peers through metad. Consider cluster-2 configuration as example:

```plain
# Meta server address
--meta_server_addrs=172.17.0.2:45500,172.17.0.3:45500,172.17.0.4:45500
# Local ip
--local_ip=172.17.0.2
# Storage daemon listening port
--port=44500
```

Repeat the above steps to configure cluster-3 and cluster-4. A total of 9 files must be configured.

## Run Clusters

Restart the service after the cluster configuration is complete:

```bash
$/usr/local/nebula/scripts/nebula.service stop all

[INFO] Stopping nebula-metad...
[INFO] Done
[INFO] Stopping nebula-graphd...
[INFO] Done
[INFO] Stopping nebula-storaged...
[INFO] Done

$/usr/local/nebula/scripts/nebula.service start all

[INFO] Starting nebula-metad...
[INFO] Done
[INFO] Starting nebula-graphd...
[INFO] Done
[INFO] Starting nebula-storaged...
[INFO] Done
```

Repeat the above command to restart cluster-3 and cluster-4.

## Test Clusters

Now use the console to log on to one of the clusters and run the following command

```bash
$/usr/local/nebula/bin/nebula -u user -p password --addr 172.17.0.2 --port 3699

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

The three shown hosts indicate that the clusters are successfully deployed.

```ngql
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

Stop the storage process of cluster-4:

```bash
$/usr/local/nebula/scripts/nebula.service stop storaged
```

Check the status of the process to confirm the process is successfully stopped:

```bash
$ /usr/local/nebula/scripts/nebula.service status storaged
[INFO] nebula-storaged: Exited
```

Log on to cluster-2 and check hosts using command `SHOW HOSTS`:

```ngql
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

At this time the status of cluster-4 is offline, indicating it has been stopped successfully.

**_Note:_** The status of 172.17.0.4 will be online due to the 10 minutes default value of `expired_threshold_sec`. You can change it in `nebula-metad.conf` and change `heartbeat_interval_secs` in `nebula-storaged.conf`. Data can only be read when most of the replicas are available.

Test whether the data is readable with one storage killed:

```ngql
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

The returned data is the same as above, indicating that the deployment is successful.

## Custom Configuration File

Nebula supports loading advanced parameters by specifying configuration files for performance tuning.
-->

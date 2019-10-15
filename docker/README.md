# Dockerfiles for Nebula Graph Services

**NOTE:** The `Dockerfile` is used to build docker image only for **testing** nebula graph in local machine.

Following Dockerfiles will be ready in production.

- `Dockerfile.graphd`: nebula-graphd service
- `Dockerfile.metad`: nebula-metad service
- `Dockerfile.storaged`: nebula-storaged service
- `Dockerfile.console`: nebula console client

## docker-compose

Use git to clone nebula project to your local directory and `cd` to `docker` folder in nebula root directory.

```shell
$ cd /path/to/nebula/root/directory/ # replace your real nebula clone path
$ cd docker
```

**Step 1**: Start all services with `docker-compose`

```shell
$ docker-compose up -d
Creating network "docker_nebula-net" with the default driver
Creating docker_metad2_1 ... done
Creating docker_metad0_1 ... done
Creating docker_metad1_1 ... done
Creating docker_graphd0_1   ... done
Creating docker_storaged2_1 ... done
Creating docker_graphd1_1   ... done
Creating docker_storaged0_1 ... done
Creating docker_graphd2_1   ... done
Creating docker_storaged1_1 ... done
```

**Step 2**: List all nebula services and check their exposed ports

``` shell
$ docker-compose ps
       Name                     Command                       State                                                   Ports
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
docker_graphd0_1     ./bin/nebula-graphd --flag ...   Up (health: starting)   0.0.0.0:32867->13000/tcp, 0.0.0.0:32866->13002/tcp, 3369/tcp, 0.0.0.0:32868->3699/tcp
docker_graphd1_1     ./bin/nebula-graphd --flag ...   Up (health: starting)   0.0.0.0:32871->13000/tcp, 0.0.0.0:32869->13002/tcp, 3369/tcp, 0.0.0.0:32875->3699/tcp
docker_graphd2_1     ./bin/nebula-graphd --flag ...   Up (health: starting)   0.0.0.0:32878->13000/tcp, 0.0.0.0:32874->13002/tcp, 3369/tcp, 0.0.0.0:32880->3699/tcp
docker_metad0_1      ./bin/nebula-metad --flagf ...   Up (health: starting)   0.0.0.0:32865->11000/tcp, 0.0.0.0:32864->11002/tcp, 45500/tcp, 45501/tcp
docker_metad1_1      ./bin/nebula-metad --flagf ...   Up (health: starting)   0.0.0.0:32863->11000/tcp, 0.0.0.0:32862->11002/tcp, 45500/tcp, 45501/tcp
docker_metad2_1      ./bin/nebula-metad --flagf ...   Up (health: starting)   0.0.0.0:32861->11000/tcp, 0.0.0.0:32860->11002/tcp, 45500/tcp, 45501/tcp
docker_storaged0_1   ./bin/nebula-storaged --fl ...   Up (health: starting)   0.0.0.0:32879->12000/tcp, 0.0.0.0:32877->12002/tcp, 44500/tcp, 44501/tcp
docker_storaged1_1   ./bin/nebula-storaged --fl ...   Up (health: starting)   0.0.0.0:32876->12000/tcp, 0.0.0.0:32872->12002/tcp, 44500/tcp, 44501/tcp
docker_storaged2_1   ./bin/nebula-storaged --fl ...   Up (health: starting)   0.0.0.0:32873->12000/tcp, 0.0.0.0:32870->12002/tcp, 44500/tcp, 44501/tcp
```

Now we can see the `graph0` exposed port is `32868`

**Step 3**: Use `nebula-console` docker container to connect to one of above **Graph Services**

``` shell
$ docker run --rm -ti --network=host vesoft/nebula-console --port=32868 --addr=127.0.0.1

Welcome to Nebula Graph (Version 49d651f)

(user@127.0.0.1) [(none)]> SHOW HOSTS;
=============================================================================================
| Ip         | Port  | Status | Leader count | Leader distribution | Partition distribution |
=============================================================================================
| 172.28.2.1 | 44500 | online | 0            |                     |                        |
---------------------------------------------------------------------------------------------
| 172.28.2.2 | 44500 | online | 0            |                     |                        |
---------------------------------------------------------------------------------------------
| 172.28.2.3 | 44500 | online | 0            |                     |                        |
---------------------------------------------------------------------------------------------
Got 3 rows (Time spent: 6479/7619 us)
```

**Step 4**: Check cluster data and logs

All nebula service data and logs are stored in local directory: `./data` and `./logs`.

```text
|-- docker-compose.yaml
|-- data
|     |- meta0
|     |- meta1
|     |- meta2
|     |- storage0
|     |- storage1
|     `- storage2
`-- logs
     |- meta0
     |- meta1
     |- meta2
     |- storage0
     |- storage1
     |- storage2
     |- graph0
     |- graph1
     |- graph2
```

Then enjoy nebula graph :)

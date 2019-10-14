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

Start all services with `docker-compose` and enter `console` container. Then enjoy nebula graph :)

```shell
$ docker-compose up -d
$ docker-compose run console bash
(user@172.28.1.3) [(none)]> SHOW HOSTS;
```

# Installing Nebula Graph with Docker-compose

Before you can install **Nebula Graph**, you must ensure that `Docker Engine` and `Docker Compose` are installed.

## Requirements

Before you install **Nebula Graph**, you must ensure that the following software and hardware requirements are met.

### Software Requirements

Ensure that your system is one of the following:

* Fedora 29, 30
* Centos, 6.5, 7.5
* Ubuntu 16.04, 18.04

### Hardware Requirements

Ensure that your disk space and memory meet the following requirements:

* 30G for disk space
* 8G for memory

## Installing Docker Engine

In this section, we will show you how to install the latest version of `Docker Engine for Community`.

You can install the latest version of `Docker Engine` by the following steps:

1. Uninstall the old versions of `Docker`.

```bash
sudo apt-get remove docker docker-engine docker.io containerd runc
```

2. Update the `apt` package index.

```bash
sudo apt-get update
```

3. Install packages to allow `apt` to use a repository over HTTPS. When your are prompted, press `y`.

``` bash
sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
    ```
4. Add Docker’s official GPG key:

```bash
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
```

5. Verify that you now have the key with the fingerprint `9DC8 5822 9FC7 DD38 854A E2D8 8D81 803C 0EBF CD88`, by searching for the last 8 characters of the fingerprint.

```bash
sudo apt-key fingerprint 0EBFCD88
```

6. Use the following command to set up the `stable` repository.

```bash
sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
```

7. Update the `apt` package index.

```bash
sudo apt-get update
```

8. Install the latest version of `Docker` and `containerd`. When you are prompted, press `y`.

```bash
sudo apt-get install docker-ce docker-ce-cli containerd.io
```

9. Verify that `Docker` is installed correctly by running the `hello-world` image.

```bash
sudo docker run hello-world
```

**Note**:

1. After entering the `sudo docker run hello-world` command, if the `Hello from Docker` message is displayed, it indicates that `Docker Engine` is installed successfully.

2. For details about installing `Docker Engine - Community`, you can refer to https://docs.docker.com/install/linux/docker-ce/ubuntu/.

## Installing Docker Compose

In this section, we will show you how to install the latest version of Docker Compose. You must install `Docker Engine` before installing `Docker Compose`.

You can install `Docker Compose` by the following steps:

1. Download the current stable release of `Docker Compose`.

```bash
sudo curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
```

2. Apply executable permissions to the binary.

```bash
sudo chmod +x /usr/local/bin/docker-compose
```

**Note**:

1. You can check `Docker Compose` by entering `docker-compose --version`. If the `docker-compose version 1.24.1, build 1110ad01` message is displayed, it indicates `Docker Compose` is installed successfully.

2. For details about installing `Docker Compose`, you can refer to https://docs.docker.com/compose/install/.

## Installing Nebula Graph

After `Docker Engine` and `Docker Compose` are installed, you can install `Nebula Graph`.

You can install **Nebula Graph** and get it up and running by the following steps.

1. Run a container in docker.

```bash
sudo docker run -it vesoft/nebula-graph:nightly /bin/bash
```

2. Change your current directory to the `nebula` directory.

```bash
cd /usr/local/nebula/
```

3. Start meta service, storage service and graph service.

```bash
scripts/nebula.service start all
```

4. Check the status of **Nebula Graph** services.

```bash
scripts/nebula.service status all
```

5. Connect to the **Nebula Graph** services.

```bash
bin/nebula -u=user -p=password
```

**Note**: If you can see `(user@127.0.0.1)[(none)]` on your console, it indicates your **Nebula Graph** services are up and running.

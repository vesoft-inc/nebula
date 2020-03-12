# **Nebula Graph** Installation with rpm/deb Package

## Overview

This guide will walk you through the process of installing **Nebula Graph** with `rpm/deb` packages.

## Prerequisites

Before getting started, ensure that you meet the following requirements:

* Hard disk: 50 GB
* Memory: 8 GB

## Installing **Nebula Graph**

To install **Nebula Graph** with a `rpm/deb` package, you must complete the following steps:

1. Log in to GitHub and click [rpm/deb](https://github.com/vesoft-inc/nebula/actions) to locate the `rpm/deb` package.

2. Under the **Actions** tab, click **package** on the left. All packages available are displayed.

3. Click the latest package on the top of the package list.

![action-page](https://user-images.githubusercontent.com/40747875/71390992-59d1be80-263d-11ea-9d61-1d7fbeb1d8c5.png)

4. Click the **Artifacts** list on the upper right corner to select a package to download.

![select-a-package](https://user-images.githubusercontent.com/40747875/71389414-415ea580-2637-11ea-8930-eaef1e8a5d17.png)

5. Install **Nebula Graph**.

* For a `rpm` file, install **Nebula Graph** with the following command:

```bash
$ sudo rpm -ivh nebula-2019.12.23-nightly.el6-5.x86_64.rpm
```

* For a `deb` file, install **Nebula Graph** with the following command:

```bash
$ sudo dpkg -i nebula-2019.12.23-nightly.ubuntu1604.amd64.deb
```

* Install **Nebula Graph** to your customized directory with the following command:

```bash
rpm -ivh --prefix=${your_dir} nebula-graph-${version}.rpm
```

* Package **Nebula Graph** to one package with the following command:

```bash
cd nebula/package
./package.sh -v <version>
```

* Package **Nebula Graph** to multiple packages with the following command:

```bash
cd nebula/package
./package.sh -v <version> -n OFF
```

**Note**:

1. Replace the above file name with your own file name, otherwise, this command might fail.
2. **Nebula Graph** is installed in the `/usr/local/nebula` directory by default.

## Starting **Nebula Graph** Services

After **Nebula Graph** is installed successfully, you can start **Nebula Graph** services with the following command:

```bash
$ sudo /usr/local/nebula/scripts/nebula.service start all
```

## Checking **Nebula Graph** Services

You can check the **Nebula Graph** services with the following command:

```bash
$ sudo /usr/local/nebula/scripts/nebula.service status all
```

## Connecting to **Nebula Graph** Services

You can connect to **Nebula Graph** services with the following command:

```bash
$ sudo /usr/local/nebula/bin/nebula -u user -p password
```

**Note**: If you successfully connect to **Nebula Graph**, the `Welcome to Nebula Graph` information is returned.

## Stopping **Nebula Graph** Services

If you want to stop **Nebula Graph** services, you can enter the following command:

```bash
$ sudo /usr/local/nebula/scripts/nebula.service stop all
```

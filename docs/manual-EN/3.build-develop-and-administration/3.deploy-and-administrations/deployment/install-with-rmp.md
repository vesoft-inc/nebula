# Nebula Graph Installation with rpm/deb Package

## Overview

This guide will walk you through the process of installing **Nebula Graph** with rpm/deb packages.

## Prerequisites

Before getting started, ensure that you meet the following requirements:

* Hard disk: 50 GB
* Memory: 8 GB

## Installing Nebula Graph

To install **Nebula Graph** with a rpm package, you must complete the following steps:

1. Log in to GitHub and click [this link](https://github.com/vesoft-inc/nebula/actions) to locate the rpm package.

2. Under the **Actions** tag, click **package** on the left. All packages available are displayed.

3. Click the latest package on the top of the package list.

4. Click the **Artifacts** list on the upper right corner to select the package that meets your system requirements.

5. Click your selected package to start downloading the package.

6. Extract the package to the selected directory after the package is downloaded.

7. Enter the following command to install **Nebula Graph**.

```shell
$ rpm -ivh nebula-5ace754.el7-5.x86_64.rpm
```

**Note**: Replace the above file name with your own file name, otherwise, this command might fail.

## Starting **Nebula Graph** Services

After **Nebula Graph** is installed successfully, you can start **Nebula Graph** services with the following command:

```shell
$ /usr/local/nebula/scripts/nebula.service start all
```

## Checking **Nebula Graph** Services

You can check the **Nebula Graph** services with the following command:

```shell
$ /usr/local/nebula/scripts/nebula.service status all
```

## Connecting to **Nebula Graph** Services

You can connect to **Nebula Graph** services with the following command:

```shell
$ /usr/local/nebula/bin/nebula -u user -p password
```

**Note**: If you successfully connect to **Nebula Graph**, the `Welcome to Nebula information` is returned.

## Stopping **Nebula Graph** Services

If you want to stop **Nebula Graph** services, you can enter the following command:

```shell
$ /usr/local/nebula/scripts/nebula.service stop all
```

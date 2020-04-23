# 使用 rpm/deb 包安装 **Nebula Graph**

## 概览

本指南将指导您使用 `rpm/deb` 包来安装 **Nebula Graph**。

## 前提条件

在开始前，请确保满足以下条件：

* 硬盘：50 GB
* 内存：8 GB

## 安装 **Nebula Graph**

 使用 `rpm/deb` 包来安装 **Nebula Graph**，需要完成以下步骤：

1. 下载安装包

- 方式一：通过 GitHub 获取安装包

	1）登录到 GitHub 并单击 [rpm/deb](https://github.com/vesoft-inc/nebula/actions) 链接。

	2）在 **Actions** 选项卡下，单击左侧的 **package**，显示所有可用的包。

	3）单击列表顶部最新的包。
	![action-page](https://user-images.githubusercontent.com/40747875/71390992-59d1be80-263d-11ea-9d61-1d7fbeb1d8c5.png)

	4）单击右上角 **Artifacts**， 选择要下载的安装包。


	![select-a-package](https://user-images.githubusercontent.com/40747875/71389414-415ea580-2637-11ea-8930-eaef1e8a5d17.png)

- 方式二：通过阿里云 OSS 获取安装包

	1）获取 release 版本，url 格式如下：

	- [Centos 6](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/${release_version}/nebula-${release_version}.el6-5.x86_64.rpm)

	- [Centos 7](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/${release_version}/nebula-${release_version}.el7-5.x86_64.rpm)

	- [Ubuntu 1604](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/${release_version}/nebula-${release_version}.ubuntu1604.amd64.deb)

	- [Ubuntu 1604](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/${release_version}/nebula-${release_version}.ubuntu1804.amd64.deb)

	* 其中 `${release_version}` 为具体的发布版本号，例如要下载 1.0.0-rc2 Centos 7 的安装包，那么可以直接通过命令下载

	```bash
	$ wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/1.0.0-rc2/nebula-1.0.0-rc2.el7-5.x86_64.rpm
	```

	2）获取 nightly 版本，url 格式如下：

	- [Centos 6](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/nightly/${date}/nebula-${date}-nightly.el6-5.x86_64.rpm)

	- [Centos 7](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/nightly/${date}/nebula-${date}-nightly.el7-5.x86_64.rpm)

	- [Ubuntu 1604](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/nightly/${date}/nebula-${date}-nightly.ubuntu1604.amd64.deb)

	- [Ubuntu 1804](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/nightly/${date}/nebula-${date}-nightly.ubuntu1804.amd64.deb)

	* 其中 `${date}` 为具体的日期，例如要下载 2020年4月1日的 Centos 7 的安装包，那么可以直接通过命令下载

	```bash
	$ wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/package/nightly/2020.04.01/nebula-2020.04.01-nightly.el7-5.x86_64.rpm
	```

2. 安装 **Nebula Graph**。

* 如果是 `rpm` 文件，使用以下命令安装 **Nebula Graph**：

```bash
$ sudo rpm -ivh nebula-2019.12.23-nightly.el6-5.x86_64.rpm
```

* 如果是 `deb` 文件，使用以下命令安装 **Nebula Graph**：

```bash
$ sudo dpkg -i nebula-2019.12.23-nightly.ubuntu1604.amd64.deb
```

* 如需安装至自定义目录，请使用以下命令：

```bash
rpm -ivh --prefix=${your_dir} nebula-graph-${version}.rpm
```

* 如需将 **Nebula Graph** 打包至一个包，请使用以下命令：

```bash
cd nebula/package
./package.sh -v <version>
```

* 如需将 **Nebula Graph** 打包至多个包，请使用以下命令：

```bash
cd nebula/package
./package.sh -v <version> -n OFF
```

**注意**：

1. 使用您自己的文件名替换以上命令中的文件名，否则以上命令可能执行失败。
2. **Nebula Graph** 默认会安装在 `/usr/local/nebula` 目录下。

## 启动 **Nebula Graph** 服务

成功安装 **Nebula Graph** 后，输入以下命令启动 **Nebula Graph** 服务：

```bash
$ sudo /usr/local/nebula/scripts/nebula.service start all
```

## 查看 **Nebula Graph** 服务

输入以下命令查看 **Nebula Graph** 服务：

```bash
$ sudo /usr/local/nebula/scripts/nebula.service status all
```

## 连接 **Nebula Graph** 服务

输入以下命令连接 **Nebula Graph** 服务：

```bash
$ sudo /usr/local/nebula/bin/nebula -u user -p password
```

**注意**：如果您成功连接了 **Nebula Graph** 服务，将返回 `Welcome to Nebula Graph` 消息。

## 停止 **Nebula Graph** 服务

输入以下命令停止 **Nebula Graph** 服务：

```bash
$ sudo /usr/local/nebula/scripts/nebula.service stop all
```

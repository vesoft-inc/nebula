# 使用 rpm/deb 包安装 **Nebula Graph**

## 概览

本指南将指导您使用 `rpm/deb` 包来安装 **Nebula Graph**。

## 前提条件

在开始前，请确保满足以下条件：

* 硬盘：50 GB
* 内存：8 GB

## 安装 **Nebula Graph**

使用 `rpm/deb` 包来安装 **Nebula Graph**，需要完成以下步骤：

1. 登录到 GitHub 并单击 [rpm/deb](https://github.com/vesoft-inc/nebula/actions) 链接。

2. 在 **Actions** 选项卡下，单击左侧的 **package**，显示所有可用的包。

3. 单击列表顶部最新的包。

![action-page](https://user-images.githubusercontent.com/40747875/71390992-59d1be80-263d-11ea-9d61-1d7fbeb1d8c5.png)

4. 单击右上角 **Artifacts**， 选择要下载的安装包。

![select-a-package](https://user-images.githubusercontent.com/40747875/71389414-415ea580-2637-11ea-8930-eaef1e8a5d17.png)

5. 安装 **Nebula Graph**。

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

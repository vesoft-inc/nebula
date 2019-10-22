###  编译器

Nebula 基于 C++14 开发，因此它需要一个支持 C++14 的编译器。

### 支持系统版本
- Fedora29, 30
- Centos6.5, Centos7.0~7.6
- Ubuntu16.04, 18.04

### 需要的存储空间

当编译类型为 **Debug** 的时候，最好预留 **30G** 磁盘空间

### 本地构建
#### 步骤 1: 克隆代码

```
bash> git clone https://github.com/vesoft-inc/nebula.git
```

#### 步骤 2 : 安装依赖

- 中国用户

    ```
    bash> cd nebula && ./build_dep.sh C
    ```

- 美国用户

    ```
    bash> cd nebula && ./build_dep.sh U
    ```
- 环境不能直接下载oss包的用户

    步骤 1:
    从本地源下载依赖和进行配置

    ```
    bash> cd nebula && ./build_dep.sh N
    ```

    步骤 2:
    从下面链接中下载对应版本的压缩包

    **中国用户**
    - [Fedora29/30](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/fedora29.tar.gz)
    - [Centos7.5](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/centos7.5.tar.gz)
    - [Centos6.5](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/centos6.5.tar.gz)
    - [Ubuntu1604](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/ubuntu16.tar.gz)
    - [Ubuntu1804](https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/ubuntu18.tar.gz)

    **美国用户**

    - [Fedora29/30](https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/fedora29.tar.gz)
    - [Centos7.5](https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/centos7.5.tar.gz)
    - [Centos6.5](https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/centos6.5.tar.gz)
    - [Ubuntu1604](https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/ubuntu16.tar.gz)
    - [Ubuntu1804](https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/ubuntu18.tar.gz)

    步骤 3:
    安装下载好的压缩包

    ```
    tar xf ${package_name}.tar.gz
    cd ${package_name} && ./install.sh
    ```

#### 步骤 3: 应用 **~/.bashrc** 修改

```
bash> source ~/.bashrc
```
#### 步骤 4: 构建

```
bash> mkdir build && cd build
bash> cmake ..
bash> make
bash> sudo make install
```

#### **构建完成**
- 如果没有任何错误信息

    ```
    [100%] Built target ....
    ```
    **编译成功！**
    
- 在安装目录 **/usr/local/nebula** 下有如下四个子目录 **etc/**, **bin/**, **scripts/** **share/**

    ```
    [root@centos7.5 nebula]# ls /usr/local/nebula/
    bin  etc  scripts  share
    ```
    **现在可以开始运行 Nebula** 。

### 使用 docker 容器构建

Nebula 提供了一个安装有完整编译环境的 docker 镜像 [vesoft/nebula-dev](https://hub.docker.com/r/vesoft/nebula-dev)，让开发者可以本地修改源码，容器内部构建和调试。只需执行如下几步便可快速参与开发：

#### 从 docker hub 拉取镜像

```shell
bash> docker pull vesoft/nebula-dev
```

#### 启动容器并将本地源码目录挂载到容器的工作目录 `/home/nebula`

```shell
bash> docker run --rm -ti \
  --security-opt seccomp=unconfined \
  -v /path/to/nebula/:/home/nebula \
  vesoft/nebula-dev \
  bash
```

其中 `/path/to/nebula/` 要替换成**本地 nebula 源码目录**。

#### 容器内编译

```shell
docker> mkdir _build && cd _build
docker> cmake ..
docker> make
docker> make install
```

#### 启动 nebula 服务

经过上述的安装后，便可以在容器内部启动 nebula 的服务，nebula 默认的安装目录为 `/usr/local/nebula`

```shell
docker> cd /usr/local/nebula
```

重命名 nebula 服务的配置文件

```shell
docker> cp etc/nebula-graphd.conf.default etc/nebula-graphd.conf
docker> cp etc/nebula-metad.conf.default etc/nebula-metad.conf
docker> cp etc/nebula-storaged.conf.default etc/nebula-storaged.conf
```

启动服务

```shell
docker> ./scripts/nebula.service start all
docker> ./bin/nebula -u user -p password --port 3699 --addr="127.0.0.1"
nebula> SHOW HOSTS;
```

### 常见问题和解决方案

- **错误信息**: `/usr/bin/ld: cannot find Scrt1.o: No such file or directory`

  **解决方案**:

    **步骤 1**: 在 **~/.bashrc** 末添加如下行

    ```
    export LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:$LIBRARY_PATH
    ```

    **步骤 2**: 应用 **~/.bashrc** 修改

    ```
    bash> source ~/.bashrc
    ```

- **错误信息**: `[ERROR] No compiler is provided in this environment. Perhaps you are running on a JRE rather than a JDK?`

    **解决方案**:
    1) 运行 `java -version` 来获取 jdk 版本信息
    2) 如果你的 jdk 版本不是 `1.8.0_xxx`，请安装

    **ubuntu**

    ```
    sudo apt-get -y install openjdk-8-jdk
    ```

    **centos**

    ```
    sudo yum -y install java-1.8.0-openjdk
    ```

    3) 在 **~/.bashrc** 末添加

    **ubuntu**
    ```
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
    export JRE_HOME=$JAVA_HOME/jre
    export CLASSPATH=$JAVA_HOME/lib:$JRE_HOME/lib:$CLASSPATH
    export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
    ```

    **centos**

    ```
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
    export JRE_HOME=$JAVA_HOME/jre
    export CLASSPATH=$JAVA_HOME/lib:$JRE_HOME/lib:$CLASSPATH
    export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
    ```

- **错误信息**: `internal error`

    **解决方案**:

    你需要自己编译第三方库，第三方库的安装路径为 **/opt/nebula/third-party**

    ```
    bash> wget https://nebula-graph-us.oss-us-west-1.aliyuncs.com/third-party/nebula-3rdparty.tar.gz
    bash> tar xf nebula-3rdparty.tar.gz && cd nebula-3rdparty
    bash> ./install_deps.sh
    bash> sudo make
    bash> cd nebula && ./build_dep.sh N
    bash> source ~/.bashrc
    bash> mkdir build && cd build
    bash> cmake ..
    bash> make
    bash> sudo make install
    ```

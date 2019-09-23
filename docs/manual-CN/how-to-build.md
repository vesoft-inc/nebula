
### 编译器

Nebula 在 C++14 上开发，因此它需要一个支持 C++14 的编译器。

### 支持系统版本
- Fedora29, 30
- Centos6.5, 7.5
- Ubuntu16.04, 18.04


### 在 Fedora29 和 Fedora30 上构建
#### 步骤 1: 准备工作
- 安装工具

    ```
    bash> sudo yum -y install git
    ```

- 安装依赖模块

    ```
    bash> sudo yum -y install gcc gcc-c++ libstdc++-static cmake make autoconf automake flex gperf libtool bison unzip boost boost-devel boost-static krb5-devel krb5-libs openssl openssl-devel libunwind libunwind-devel ncurses ncurses-devel readline readline-devel python java-1.8.0-openjdk java-1.8.0-openjdk-devel
    ```

#### 步骤 2: 构建和安装第三方库
第三方库被安装在 **/opt/nebula/third-party**

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
bash> cd nebula-3rdparty
bash> cmake ./
bash> cmake -DSKIP_JAVA_JAR=OFF  ./  # if you need to build java client
bash> make
bash> sudo make install
```

#### 步骤 3: 构建 Nebula
默认的安装路径是 **/usr/local/nebula**

```
bash> git clone https://github.com/vesoft-inc/nebula.git
bash> cd nebula && mkdir build && cd build
bash> cmake ..
bash> cmake -DSKIP_JAVA_CLIENT=OFF ..  # if you need to build java client
bash> make
bash> sudo make install
```

### 在 Centos7.5 上构建

#### 步骤 1: 准备工作
- 安装工具

    ```
    bash> sudo yum -y install git
    ```
- 安装依赖模块

    通过 yum install

    ```
    bash> sudo yum install -y libtool autoconf autoconf-archive automake perl-WWW-Curl libstdc++-static ncurses ncurses-devel readline readline-devel maven java-1.8.0-openjdk
    ```

    和通过 VESoft Inc. 提供的包

    ```
    # From China
    bash> wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/centos-7.5-1804.tar.gz
    # From US
    bash> wget https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/centos-7.5-1804.tar.gz
    ```

    1) 建立一个系统用户, 并将其 home 目录设置为共享目录

    ```
    bash> sudo adduser --system --group --home /home/engshare engshare
    ```

    2) 确保 home 目录 **/home/engshare** 对全用户可读

    ```
    bash> chmod -R 755 /home/engshare
    ```

    3) 在此目录下安装所有需要的 rpm 包

    ```
    bash> tar xf centos-7.5-1804.tar.gz && cd centos-7.5-1804/
    bash> rpm -ivh *.rpm
    ```

    4) 在 **~/.bashrc** 末添加如下几行

    ```
    alias cmake='/home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind'

    alias ctest='/home/engshare/cmake/bin/ctest'
    ```
    5) 应用 **~/.bashrc** 修改

    ```
    bash> source ~/.bashrc
    ```

#### 步骤 2: 构建和安装第三方库
第三方库被安装在 **/opt/nebula/third-party**

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
bash> cd nebula-3rdparty
bash> cmake ./
bash> cmake -DSKIP_JAVA_JAR=OFF  ./  # if you need to build java client
bash> make
bash> sudo make install
```

#### 步骤 3: 构建 Nebula
默认的安装路径是 **/usr/local/nebula**

```
bash> git clone https://github.com/vesoft-inc/nebula.git
bash> cd nebula && mkdir build && cd build
bash> cmake ..
bash> cmake -DSKIP_JAVA_CLENT=OFF ..  # if you need to build java client
bash> make
bash> sudo make install
```

### 在 Centos6.5 上构建

#### 步骤 1: 准备工作
- 安装工具

    ```
    bash> sudo yum -y install git
    ```
- 安装依赖模块

    通过 yum install

    ```
    bash> sudo yum -y install libtool autoconf autoconf-archive automake perl-WWW-Curl perl-YAML perl-CGI glibc-devel libstdc++-static ncurses ncurses-devel readline readline-devel maven java-1.8.0-openjdk
    ```

    和通过 VESoft Inc. 提供的包

    ```
    # From China
    bash> wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/centos-6.5.tar.gz
    # From US
    bash> wget https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/centos-6.5.tar.gz
    ```

    1) 在此目录下安装所有需要的 rpm 包

    ```
    bash> tar xf centos-6.5.tar.gz && cd centos-6.5/
    bash> sudo rpm -ivh *.rpm
    ```

    2) 在 **~/.bashrc** 末添加如下几行

    ```
    export PATH=/opt/nebula/autoconf/bin:/opt/nebula/automake/bin:/opt/nebula/libtool/bin:/opt/nebula/git/bin:/opt/nebula/gettext/bin:/opt/nebula/flex/bin:/opt/nebula/bison/bin:/opt/nebula/binutils/bin:$PATH
    export ACLOCAL_PATH=/opt/nebula/automake/share/aclocal-1.15:/opt/nebula/libtool/share/aclocal:/opt/nebula/autoconf-archive/share/aclocal
    
    alias cmake='/opt/nebula/cmake/bin/cmake -DCMAKE_C_COMPILER=/opt/nebula/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/opt/nebula/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/opt/nebula/gperf/bin -DNEBULA_FLEX_ROOT=/opt/nebula/flex -DNEBULA_BISON_ROOT=/opt/nebula/bison -DNEBULA_BOOST_ROOT=/opt/nebula/boost -DNEBULA_OPENSSL_ROOT=/opt/nebula/openssl -DNEBULA_KRB5_ROOT=/opt/nebula/krb5 -DNEBULA_LIBUNWIND_ROOT=/opt/nebula/libunwind'
    alias ctest='/opt/nebula/cmake/bin/ctest'
    ```
    3) 应用 **~/.bashrc** 修改

    ```
    bash> source ~/.bashrc
    ```

#### 步骤 2: 构建和安装第三方库
第三方库被安装在 **/opt/nebula/third-party**

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
bash> cd nebula-3rdparty
bash> cmake ./
bash> cmake -DSKIP_JAVA_JAR=OFF  ./  # if you need to build java client
bash> make
bash> sudo make install
```

#### 步骤 3: 构建 Nebula
默认的安装路径是 **/usr/local/nebula**

```
bash> git clone https://github.com/vesoft-inc/nebula.git
bash> cd nebula && mkdir build && cd build
bash> cmake ..
bash> cmake -DSKIP_JAVA_CLENT=OFF ..  # if you need to build java client
bash> make
bash> sudo make install
```

### 在 Ubuntu18.04 和 Ubuntu16.04 上构建

#### 步骤 1: 准备工作
- 安装工具

    ```
    bash> sudo apt-get -y install git
    ```
- 安装依赖模块

    通过 apt-get install

    ```
    bash> sudo apt-get -y install gcc-multilib libtool autoconf autoconf-archive automake libncurses5-dev libreadline-dev python maven java-1.8.0-openjdk
    ```

    通过 VESoft Inc. 提供的包

    ```
    # From China
    bash> wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/ubuntu1804.tar.gz
    # From US
    bash> wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/ubuntu1804.tar.gz
    ```

    1) 建立一个系统用户, 并将其 home 目录设置为共享目录

    ```
    bash> sudo adduser --system --group --home /home/engshare engshare
    ```

    2) 确保 home 目录 **/home/engshare** 对全用户可读

    ```
    bash> chmod -R 755 /home/engshare
    ```

    3) 在此目录下安装所有需要的 deb 包

    ```
    bash> tar xf ubuntu1804.tar.gz && cd ubuntu1804/
    bash> sudo dpkg -i *.deb
    ```

    4)  在 **~/.bashrc** 末添加如下几行

    ```
    alias cmake='/home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind'
    
    alias ctest='/home/engshare/cmake/bin/ctest'
    ```
    5) 应用 **~/.bashrc** 修改

    ```
    bash> source ~/.bashrc
    ```

#### 步骤 2: 构建和安装第三方库
第三方库被安装在 **/opt/nebula/third-party**

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
bash> cd nebula-3rdparty
bash> cmake
bash> cmake -DSKIP_JAVA_JAR=OFF  ./  # if you need to build java client
bash> make
bash> sudo make install
```

#### 步骤 3: 构建 Nebula
默认的安装路径是 **/usr/local/nebula**

```
bash> git clone https://github.com/vesoft-inc/nebula.git
bash> cd nebula && mkdir build && cd build
bash> cmake ..
bash> cmake -DSKIP_JAVA_CLENT=OFF ..  # if you need to build java client
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

- **错误信息**: `bison verson less than 3.0.5`

    **解决方案**:

    1) 下载 bison-3.0.5.tar.gz

    ```
    bash> wget http://ftp.gnu.org/gnu/bison/bison-3.0.5.tar.gz
    ```

    2) 构建和安装

    ```
    bash> ./configure
    bash> make && make install

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

---
This tutorial provides an introduction to building `Nebula` code.
---

### Compiler

The project Nebula is developed using C++14, so it requires a compiler supporting C++14 features.

### Support system version
- Fedora29, 30
- Centos6.5, 7.5
- Ubuntu16.04, 18.04

### The amount of disk space required

When build type is **DEBUG**, **Nebula-3rdparty** and **nebula** build require approximately **13G** of disk space to compile.
Please allow for adequate disk space. Better reserve more than **16G** of space.

### How to build in Fedora29 or Fedora30
#### Step1: Prepare
- Install tools

    ```
    bash> sudo yum -y install git
    ```

- Install dependencies

    ```
    bash> sudo yum -y install gcc gcc-c++ libstdc++-static cmake make autoconf automake flex gperf libtool bison unzip boost boost-devel boost-static krb5-devel krb5-libs openssl openssl-devel libunwind libunwind-devel ncurses ncurses-devel readline readline-devel python java-1.8.0-openjdk java-1.8.0-openjdk-devel
    ```

#### Step2: Build and install 3rd-party Libraries
The third-party was installed to **/opt/nebula/third-party**

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
bash> cd nebula-3rdparty
bash> cmake ./
bash> cmake -DSKIP_JAVA_JAR=OFF  ./  # if you need to build java client
bash> make
bash> sudo make install
```

If you want to build the java client, you should run the following command:

```
cd third-party/fbthrift/thrift/lib/java/thrift
mvn compile install
```

#### Step3: Build nebula
The default installation directory is **/usr/local/nebula**

```
bash> git clone https://github.com/vesoft-inc/nebula.git
bash> cd nebula && mkdir build && cd build
bash> cmake ..
bash> cmake -DSKIP_JAVA_CLIENT=OFF ..  # if you need to build java client
bash> make
bash> sudo make install
```

### How to build in Centos7.5

#### Step1: Prepare
- Install tools

    ```
    bash> sudo yum -y install git
    ```
- Install dependencies

    through yum install

    ```
    bash> sudo yum install -y libtool autoconf autoconf-archive automake ncurses ncurses-devel readline readline-devel perl-WWW-Curl libstdc++-static maven java-1.8.0-openjdk
    ```

    and through VESoft Inc. offer

    ```
    # From China
    bash> wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/centos-7.5-1804.tar.gz
    # From US
    bash> wget https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/centos-7.5-1804.tar.gz
    ```

    1) Create a system user, which home directory will be used as a shared file location

    ```
    bash> sudo adduser --system --group --home /home/engshare engshare
    ```

    2) Please make sure the user's home directory **/home/engshare** is readable to all users

    ```
    bash> chmod -R 755 /home/engshare
    ```

    3) Install all necessary rpm packages in this folder

    ```
    bash> tar xf centos-7.5-1804.tar.gz && cd centos-7.5-1804/
    bash> rpm -ivh *.rpm
    ```

    4) Modify **~/.bashrc** by appending following lines to the end

    ```
    alias cmake='/home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind'
    alias ctest='/home/engshare/cmake/bin/ctest'
    ```

    5) Update **~/.bashrc**

    ```
    bash> source ~/.bashrc
    ```

#### Step2: Build and install 3rd-party Libraries
The third-party was installed to **/opt/nebula/third-party**

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
bash> cd nebula-3rdparty
bash> cmake ./
bash> cmake -DSKIP_JAVA_JAR=OFF  ./  # if you need to build java client
bash> make
bash> sudo make install
```

#### Step3: Build nebula
The default installation directory is **/usr/local/nebula**

```
bash> git clone https://github.com/vesoft-inc/nebula.git
bash> cd nebula && mkdir build && cd build
bash> cmake ..
bash> cmake -DSKIP_JAVA_CLENT=OFF ..  # if you need to build java client
bash> make
bash> sudo make install
```

### How to build in Centos6.5

#### Step1: Prepare
- Install tools

    ```
    bash> sudo yum -y install git
    ```
- Install dependencies

    through yum install

    ```
    bash> sudo yum -y install libtool autoconf autoconf-archive automake perl-WWW-Curl perl-YAML perl-CGI glibc-devel libstdc++-static ncurses ncurses-devel readline readline-devel maven java-1.8.0-openjdk
    ```

    and through VESoft Inc. offer

    ```
    # From China
    bash> wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/centos-6.5.tar.gz
    # From US
    bash> wget https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/centos-6.5.tar.gz

    ```

    1) Install all necessary rpm packages in this folder

    ```
    bash> tar xf centos-6.5.tar.gz && cd centos-6.5/
    bash> sudo rpm -ivh *.rpm
    ```

    2) Modify **~/.bashrc** by appending following lines to the end

    ```
    export PATH=/opt/nebula/autoconf/bin:/opt/nebula/automake/bin:/opt/nebula/libtool/bin:/opt/nebula/git/bin:/opt/nebula/gettext/bin:/opt/nebula/flex/bin:/opt/nebula/bison/bin:/opt/nebula/binutils/bin:$PATH
    export ACLOCAL_PATH=/opt/nebula/automake/share/aclocal-1.15:/opt/nebula/libtool/share/aclocal:/opt/nebula/autoconf-archive/share/aclocal
    ```
    ```
    alias cmake='/opt/nebula/cmake/bin/cmake -DCMAKE_C_COMPILER=/opt/nebula/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/opt/nebula/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/opt/nebula/gperf/bin -DNEBULA_FLEX_ROOT=/opt/nebula/flex -DNEBULA_BISON_ROOT=/opt/nebula/bison -DNEBULA_BOOST_ROOT=/opt/nebula/boost -DNEBULA_OPENSSL_ROOT=/opt/nebula/openssl -DNEBULA_KRB5_ROOT=/opt/nebula/krb5 -DNEBULA_LIBUNWIND_ROOT=/opt/nebula/libunwind'
    alias ctest='/opt/nebula/cmake/bin/ctest'
    ```

    3) Update **~/.bashrc**

    ```
    bash> source ~/.bashrc
    ```

#### Step2: Build and install 3rd-party Libraries
The third-party was installed to **/opt/nebula/third-party**

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
bash> cd nebula-3rdparty
bash> cmake ./
bash> cmake -DSKIP_JAVA_JAR=OFF  ./  # if you need to build java client
bash> make
bash> sudo make install
```

#### Step3: Build nebula
The default installation directory is **/usr/local/nebula**

```
bash> git clone https://github.com/vesoft-inc/nebula.git
bash> cd nebula && mkdir build && cd build
bash> cmake ..
bash> cmake -DSKIP_JAVA_CLENT=OFF ..  # if you need to build java client
bash> make
bash> sudo make install
```

### How to build in Ubuntu18.04 or Ubuntu16.04

#### Step1: Prepare
- Install tools

    ```
    bash> sudo apt-get -y install git
    ```
- Install dependencies

    through apt-get install

    ```
    bash> sudo apt-get -y install gcc-multilib libtool autoconf autoconf-archive automake libncurses5-dev libreadline-dev python maven openjdk-8-jdk
    ```

    and through VESoft Inc. offer

    ```
    # From China
    bash> wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/ubuntu1804.tar.gz
    # From US
    bash> wget https://nebula-graph-cn.oss-us-west-1.aliyuncs.com/build-deb/ubuntu1804.tar.gz
    ```

    1) Create a system user, which home directory will be used as a shared file location

    ```
    bash> sudo adduser --system --group --home /home/engshare engshare
    ```

    2) Please make sure the user's home directory **/home/engshare** is readable to all users

    ```
    bash> chmod -R 755 /home/engshare
    ```

    3) Install all necessary deb packages in this folder

    ```
    bash> tar xf ubuntu1804.tar.gz && cd ubuntu1804/
    bash> sudo dpkg -i *.deb
    ```

    4) Modify **~/.bashrc** by appending following lines to the end

    ```
    alias cmake='/home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind'
    ```

    ```
    alias ctest='/home/engshare/cmake/bin/ctest'
    ```

    5) Update **~/.bashrc**

    ```
    bash> source ~/.bashrc
    ```

#### Step2: Build and install 3rd-party Libraries
The third-party was installed to **/opt/nebula/third-party**

```
bash> git clone https://github.com/vesoft-inc/nebula-3rdparty.git
bash> cd nebula-3rdparty
bash> cmake
bash> cmake -DSKIP_JAVA_JAR=OFF  ./  # if you need to build java client
bash> make
bash> sudo make install
```

#### Step3: Build nebula
The default installation directory is **/usr/local/nebula**

```
bash> git clone https://github.com/vesoft-inc/nebula.git
bash> cd nebula && mkdir build && cd build
bash> cmake ..
bash> cmake -DSKIP_JAVA_CLIENT=OFF ..  # if you need to build java client
bash> make
bash> sudo make install
```

#### **Now build finish**
- If you don't see any error messages，and the end has

    ```
    [100%] Built target ....
    ```
    **Congratulations! Compile successfully...**
- You can see the installation directory **/usr/local/nebula** with four folders **etc/**, **bin/**, **scripts/** **share/** below

    ```
    [root@centos7.5 nebula]# ls /usr/local/nebula/
    bin  etc  scripts  share
    ```
    
    **Now, you can start nebula!** [Getting Started](get-started.md)



### Problems and solutions you may encounter

- **Error info**: `/usr/bin/ld: cannot find Scrt1.o: No such file or directory`

  **resolve**:

    **Step1**: Modify **~/.bashrc** by appending following line to the end

    ```
    export LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:$LIBRARY_PATH
    ```

    **Step2**: Update **~/.bashrc**

    ```
    bash> source ~/.bashrc
    ```

- **Error info**: `bison verson less than 3.0.5`

    **resolve**:

    1) download bison-3.0.5.tar.gz

    ```
    bash> wget http://ftp.gnu.org/gnu/bison/bison-3.0.5.tar.gz
    ```

    2) build and install

    ```
    bash> ./configure
    bash> make && make install

    ```

- **Error info**: `[ERROR] No compiler is provided in this environment. Perhaps you are running on a JRE rather than a JDK?`

    **resolve**:

    1) do `java -version` get your java jdk version

    2) if your java version is not `1.8.0_xxx`, please install it

    **ubuntu**

    ```
    sudo apt-get -y install openjdk-8-jdk
    ```
    **centos**

    ```
    sudo yum -y install java-1.8.0-openjdk
    ```
    3) switch java

    ```
    sudo update-alternatives --config java
    ```
    and select the java-1.8.0-openjdk/java-8-openjdk

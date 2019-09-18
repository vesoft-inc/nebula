###  编译器

Nebula 在 C++14 上开发，因此它需要一个支持 C++14 的编译器。

### 支持系统版本
- Fedora29, 30
- Centos6.5, 7.5
- Ubuntu16.04, 18.04

### 需要的存储空间

当编译类型为**DEBUG**的时候，最好预留**30G**磁盘空间

### 构建
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

#### 步骤 3: 应用 **~/.bashrc** 修改

```
bash> source ~/.bashrc
```
#### 步骤 4: 构建

- 不编译java client

    ```
    bash> mkdir build && cd build
    bash> cmake ..
    bash> make
    bash> sudo make install
    ```
- 编译java client

    ```
    bash> mvn install:install-file -Dfile=/opt/nebula/third-party/fbthrift/thrift-1.0-SNAPSHOT.jar -DgroupId="com.facebook" -DartifactId="thrift" -Dversion="1.0-SNAPSHOT" -Dpackaging=jar
    bash> mkdir build && cd build
    bash> cmake -DSKIP_JAVA_CLIENT=OFF ..
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

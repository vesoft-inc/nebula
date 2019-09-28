---
This tutorial provides an introduction to building `Nebula` code.
---

### Compiler

Nebula is developed based C++14, so it requires a compiler supporting C++14 features.

### Supported system version
- Fedora29, 30
- Centos6.5, 7.5
- Ubuntu16.04, 18.04

### Required storage

When building type is **DEBUG**, suggestion reserve disk space is **30G** at least.

### Building locally
#### Step 1: clone code

```
bash> git clone https://github.com/vesoft-inc/nebula.git
```

#### Step 2: install dependencies
- user in China

    ```
    bash> cd nebula && ./build_dep.sh C
    ```

- user in US

    ```
    bash> cd nebula && ./build_dep.sh U
    ```

#### Step 3: update **~/.bashrc**

```
bash> source ~/.bashrc
```
#### Step 4: build
- without java client

    ```
    bash> mkdir build && cd build
    bash> cmake ..
    bash> make
    bash> sudo make install
    ```
- with java client

    ```
    bash> mvn install:install-file -Dfile=/opt/nebula/third-party/fbthrift/thrift-1.0-SNAPSHOT.jar -DgroupId="com.facebook" -DartifactId="thrift" -Dversion="1.0-SNAPSHOT" -Dpackaging=jar
    bash> mkdir build && cd build
    bash> cmake -DSKIP_JAVA_CLIENT=OFF ..
    bash> make
    bash> sudo make install
    ```

#### **Building completed**
- If no errors are shown

    ```
    [100%] Built target ....
    ```
    **Congratulations! Compile successfully...**
- You can see there are four folders in the the installation directory **/usr/local/nebula**: **etc/**, **bin/**, **scripts/**, **share/**.

    ```
    [root@centos7.5 nebula]# ls /usr/local/nebula/
    bin  etc  scripts  share
    ```
    
    **Now, you can start nebula!** [Getting Started](get-started.md)

### Building with Docker container

Nebula has provided a docker image with the whole compiling environment [vesoft/nebula-dev](https://hub.docker.com/r/vesoft/nebula-dev), which will make it possible to change source code locally, build and debug within the container. Performing the following steps to start quick development:

#### Pull image from Docker Hub

```shell
bash> docker pull vesoft/nebula-dev
```

#### Run docker container and mount your local source code directory into the container working_dir `/home/nebula`

```shell
bash> docker run --rm -ti \
  --security-opt seccomp=unconfined \
  -v /path/to/nebula/:/home/nebula \
  vesoft/nebula-dev \
  bash
```

 Replace `/path/to/nebula/` with your **local nebula source code directory**.

#### Compiling within the container

```shell
docker> mkdir _build && cd _build
docker> cmake ..
docker> make
docker> make install
```

#### Run nebula service

Once the preceding installation is completed, you can run nebula service within the container, the default installation directory is `/usr/local/nebula`.

```shell
docker> cd /usr/local/nebula
```

Rename config files of nebula service 

```shell
docker> cp etc/nebula-graphd.conf.default etc/nebula-graphd.conf
docker> cp etc/nebula-metad.conf.default etc/nebula-metad.conf
docker> cp etc/nebula-storaged.conf.default etc/nebula-storaged.conf
```

Start service

```shell
docker> ./scripts/nebula.service start all
docker> ./bin/nebula -u user -p password --port 3699 --addr="127.0.0.1"
nebula> SHOW HOSTS;
```





### FAQs

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

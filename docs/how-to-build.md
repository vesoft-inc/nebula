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

### How to build
#### Step1: clone code

```
bash> git clone https://github.com/vesoft-inc/nebula.git
```

#### Step2: install dependencies
- user in China

    ```
    bash> cd nebula && ./build_dep.sh C
    ```

- user in US

    ```
    bash> cd nebula && ./build_dep.sh U
    ```

#### Step3: update **~/.bashrc**

```
bash> source ~/.bashrc
```
#### Step4: build
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

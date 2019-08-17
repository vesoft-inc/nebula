
---
本教程旨在对 **Nebula Graph** 的使用做简要介绍。

##  安装Nebula Graph

### 通过Docker

启动 **Nebula Graph** 最快捷的方式是使用`docker`。首先，查看电脑是否已安装Docker：

```
> docker --version
Docker version 18.09.2, build 6247962
```

如果未安装，参看[这里](https://docs.docker.com/install/)。
安装之后，运行：

```
> docker pull vesoft/nebula-graph:latest
ac9208207ada: Pulling fs layer
cdcb67189ff7: Pulling fs layer
80407c3cb6b4: Pulling fs layer
latest: Pulling from vesoft/nebula-graph
ac9208207ada: Pull complete
cdcb67189ff7: Pull complete
80407c3cb6b4: Pull complete
Digest: sha256:72a73c801685595776779291969b57ab758f53ebd9bda8bab56421e50bfee161
Status: Downloaded newer image for vesoft/nebula-graph:latest
```
下载Nebula镜像。

国内从 Docker Hub 拉取镜像有时会遇到困难，此时可以配置镜像。例如:
* Azure 中国镜像 https://dockerhub.azk8s.cn
* 七牛云 https://reg-mirror.qiniu.com

linux用户请在/etc/docker/daemon.json 中写入如下内容（如果文件不存在请新建该文件

```
{
  "registry-mirrors": [
    "https://dockerhub.azk8s.cn",
    "https://reg-mirror.qiniu.com"
  ]
}
```

macOS 用户请点击任务栏中Docker Desktop图标 -> Preferences -> Daemon -> Registry mirrors。 在列表中添加 https://dockerhub.azk8s.cn 和 https://reg-mirror.qiniu.com。修改完成后，点击 Apply & Restart 按钮， 重启Docker。

镜像下载完成后，键入命令`docker images`检查镜像状态。

```
> docker images
REPOSITORY            TAG                 IMAGE ID            CREATED             SIZE
vesoft/nebula-graph   latest              1afd60e223ca        4 weeks ago         401MB
```

<!-- If `docker` is slow when pulling the image, configure a new mirror.

You can add the source at `/etc/docker/daemon.json`, for Linux users:

```
{
  "registry-mirrors": [
    "https://dockerhub.azk8s.cn",
    "https://reg-mirror.qiniu.com"
  ]
}
```

For macOS users, click the Docker Desktop icon -> Preferences -> Daemon -> Registry mirrors. Add https://dockerhub.azk8s.cn and https://reg-mirror.qiniu.com to the list. Once the modification is complete, click the Apply & Restart button to restart Docker. -->


---

**运行 Nebula Graph**

`nebula` 镜像下载完成后，运行

```
> docker run -it vesoft/nebula-graph:latest /bin/bash
```

启动docker容器。

进入容器后，默认在`root`目录下，使用`cd ~ / nebula-graph /`切换到`neula`主目录。

运行

```
> ./start-all.sh
Starting MetaService StorageService and GraphService ...
```

启动测试服务，存储服务和图服务。

运行

```
> ps -ef | grep nebula
```

查看服务进程，，如果看到nebula-metad, nebula-storaged,  nebula-graphd ，则表明服务已成功连接。

<!-- `bin/nebula` is a `console` which can be used to insert and query data.

`--port` is used for specifying the graph server port and the default value is `3699`.

`-u` and `-p` are used to specify the user name and password, `user` and `password` are the default authority.

Run

```
 > bin/nebula --port=3699 -u=user -p=password
```

to connect to the graph server. -->

运行以下命令连接nebula

```
> ./start-console.sh

Welcome to Nebula Graph (Version 0.1)

nebula>
```

如果对部署过程有任何问题，欢迎在[GitHub](https://github.com/vesoft-inc/nebula/issues)上提issue。

### 编译源码(Linux)

**依赖**

nebula遵循c++14标准，依赖第三方库：

-	autoconf
-	automake
-	libtool
-	cmake
-	bison
-	unzip
-	boost
-	gperf
-	krb5
-	openssl
-	libunwind
-	ncurses
-	readline

建议安装g++ 5以上linux系统, 比如Fedora 29。目前，nebula使用`git-lfs`存储第三方库，请确保获取源代码之前您已安装`git-lfs`。

**从GitHub获取源码**

```
> git clone https://github.com/vesoft-inc/nebula.git
```

**编译**

```
> cmake ./
```

默认安装到 /usr/local路径，如需指定安装路径，请使用:

```
> cmake -DCMAKE_INSTALL_PREFIX=$your_nebula_install_dir
```

替换此处的 `$your_nebula_install_dir`。

然后运行以下命令：

```
> make && make install
```

**运行nebula（单机）**

配置nebula-metad.conf

在nebula安装目录下，运行以下命令：

```
> cp etc/nebula-metad.conf.default etc/nebula-metad.conf
```

根据实际修改nebula-metad.conf中的配置：

- local_ip ip地址
- port 端口号
- ws_http_port metaservice HTTP HTTP服务端口号
- ws_h2_port metaservice HTTP2服务端口号


配置nebula-storaged.conf

```
> cp etc/nebula-storaged.conf.default etc/nebula-storaged.conf
```

根据实际修改nebula-storaged.conf中的配置：

- local_ip ip地址
- port 端口号
- ws_http_port storageservice HTTP服务端口号
- ws_h2_port storageservice HTTP2服务端口号

根据实际修改nebula-graphd.conf中的配置：

```
> cp etc/nebula-graphd.conf.default etc/nebula-graphd.conf
```

- local_ip ip地址
- port 端口号
- ws_http_port graphservice HTTP服务端口号
- ws_h2_port graphservice HTTP2服务端口号

**启动服务**

```
> scripts/nebula.service start all
```

查看服务状态

```
> scripts/nebula.service status all
```

**连接nebula**

```
> bin/nebula -u=user -p=password
```

- -u为用户名，默认值为`user`
- -p为密码，用户`user`的默认密码为`password`

成功连接后，添加hosts信息：

```
> ADD HOSTS $storage_ip:$storage_port
```

根据nebula-storaged.conf中的`local_ip`， `port` 替换此处的`$storage_ip`，`$storage_port`。

---

## 创建图数据

本节介绍如何构建图数据并进行查询。本示例基于下图构建：


![Untitled Diagram (1)](https://user-images.githubusercontent.com/51590253/60649144-0774c980-9e74-11e9-86d6-bad1653e70ba.png)

示例数据有三种类型的标签（_course_， _building_，_student_），两种类型的边（_select_ 和 _like_），其schema为：

```json
{  
   "tags":{  
      "course":[  
         "name: string",
         "credits: integer"
      ],
      "building":[  
         "name: string"
      ],
      "student":[  
         "name: string",
         "age: integer",
         "gender: string"
      ]
   },
   "edges":{  
      "select":[  
         "grade: integer"
      ],
      "like":[  
         "likeness: double"
      ]
   }
}
```


### 创建图空间

nebula中的图存储于 **SPACE** 中，每个space是一个物理隔离的空间。首先，需要创建一个space，然后指定使用该space以完成之后的操作。

列出已有的space：

```
nebula> SHOW SPACES;
```

创建一个名为myspace_test2的新space：

```
nebula> CREATE SPACE myspace_test2(partition_num=1, replica_factor=1);

-- 使用这个space
nebula> USE myspace_test2;
```

`replica_factor` 用来指定集群复本数。

`partition_num` 用来指定一个复本中的分区数量。
### 定义图数据Schema

使用`CREATE TAG`语句定义带有标签类型和属性列表的标签。

```
nebula> CREATE TAG course(name string, credits int);
nebula> CREATE TAG building(name string);
nebula> CREATE TAG student(name string, age int, gender string);
```

使用`CREATE EDGE`语句定义边类型。

```
nebula> CREATE EDGE like(likeness double);
nebula> CREATE EDGE select(grade int);
```

查看上述创建的标签和边：

```
-- 查看标签列表
nebula> SHOW TAGS;

-- 查看边列表
nebula> SHOW EDGES;
```

查看标签或边的属性

```
-- 查看标签属性
nebula> DESCRIBE TAG student;

-- 查看边属性
nebula> DESCRIBE EDGE like;
```


### 插入数据

为上述图数据插入点和边。

```

-- 插入点
nebula> INSERT VERTEX student(name, age, gender) VALUES 200:("Monica", 16, "female");
nebula> INSERT VERTEX student(name, age, gender) VALUES 201:("Mike", 18, "male");
nebula> INSERT VERTEX student(name, age, gender) VALUES 202:("Jane", 17, "female");
nebula> INSERT VERTEX course(name, credits),building(name) VALUES 101:("Math", 3, "No5");
nebula> INSERT VERTEX course(name, credits),building(name) VALUES 102:("English", 6, "No11");

```

```
-- 插入边
nebula> INSERT EDGE select(grade) VALUES 200 -> 101:(5);
nebula> INSERT EDGE select(grade) VALUES 200 -> 102:(3);
nebula> INSERT EDGE select(grade) VALUES 201 -> 102:(3);
nebula> INSERT EDGE select(grade) VALUES 202 -> 102:(3);
nebula> INSERT EDGE like(likeness) VALUES 200 -> 201:(92.5);
nebula> INSERT EDGE like(likeness) VALUES 201 -> 200:(85.6);
nebula> INSERT EDGE like(likeness) VALUES 201 -> 202:(93.2);
```

## 示例查询

Q1. 查询点201喜欢的点：

```
nebula> GO FROM 201 OVER like;

=======
|  id |
=======
| 200 |
-------
| 202 |
-------
```

Q2. 查询点201喜欢的点，并筛选出年龄大于17岁的点，并返回其姓名，年龄，性别，将其重全名为Friend，Age，Gender。

```
nebula> GO FROM 201 OVER like WHERE $$.student.age >= 17 YIELD $$.student.name AS Friend, $$.student.age AS Age, $$.student.gender AS Gender;

=========================
| Friend | Age | Gender |
=========================
|   Jane |  17 | female |
-------------------------
```

`YIELD`用来指定目标返回值。

`$^`为起始点。

`$$` 为目标点。

Q3. 查询点201喜欢的点选择了哪些课程和其对应年级。
```

-- 使用管道
nebula> GO FROM 201 OVER like | GO FROM $-.id OVER select YIELD $^.student.name AS Student, $$.course.name AS Course, select.grade AS Grade;

=============================
| Student |  Course | Grade |
=============================
|  Monica |    Math |     5 |
-----------------------------
|  Monica | English |     3 |
-----------------------------
|    Jane | English |     3 |
-----------------------------

-- 使用临时变量
nebula> $a=GO FROM 201 OVER like; GO FROM $a.id OVER select YIELD $^.student.name AS Student, $$.course.name AS Course, select.grade AS Grade;

=============================
| Student |  Course | Grade |
=============================
|  Monica |    Math |     5 |
-----------------------------
|  Monica | English |     3 |
-----------------------------
|    Jane | English |     3 |
-----------------------------

```

`|` 表示管道，上一个查询的输出可作为下一个管道的输入。

`$-` 表示输入流。

第二种方法使用了用户定义变量`$ a`，此变量仅适用于复合语句。

<!-- 有关查询语言的更多详细信息，请查看check [nGQL Query Language](nGQL-tutorial.md)。 -->

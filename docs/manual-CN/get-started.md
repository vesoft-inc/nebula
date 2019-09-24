# Quick Start

##  安装Nebula Graph

### Docker 方式

Docker 安装方法请参考 [Docker 官方文档](https://docs.docker.com/)。

从 Docker 镜像仓库获取最新 [Nebula Graph镜像](https://hub.docker.com/r/vesoft/nebula-graph)：

```
> docker pull vesoft/nebula-graph:latest
```

国内从 Docker Hub 拉取镜像时可能会遇到下载过慢问题，此时可以通过配置国内地址解决。例如:

* Azure 中国镜像 https://dockerhub.azk8s.cn
* 七牛云 https://reg-mirror.qiniu.com

Linux 用户请在 `/etc/docker/daemon.json` 中加入如下内容（若文件不存在，请新建该文件)

```
{
  "registry-mirrors": [
    "https://dockerhub.azk8s.cn",
    "https://reg-mirror.qiniu.com"
  ]
}
```

macOS 用户请点击任务栏中 `Docker Desktop图标 -> Preferences -> Daemon -> Registry mirrors`。 在列表中添加 https://dockerhub.azk8s.cn 和 https://reg-mirror.qiniu.com 。修改后，点击 `Apply & Restart` 按钮， 重启Docker。

镜像下载完成后，键入命令`docker images`查看下载完成的镜像。

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


**运行 Nebula Graph**

`nebula` 镜像下载完成后，运行容器：

```
> docker run -it vesoft/nebula-graph:latest /bin/bash
```

进入容器后，默认在根目录下 `/` ，切换到 `Nebula` 主目录:

```
> cd /usr/local/nebula/
```

启动 metad，storaged 和 graphd 服务:

```
> scripts/nebula.service start all
```

查看服务状态:

```
> scripts/nebula.service status all
```

<!-- `bin/nebula` is a `console` which can be used to insert and query data.

`--port` is used for specifying the graph server port and the default value is `3699`.

`-u` and `-p` are used to specify the user name and password, `user` and `password` are the default authority.

Run

```
 > bin/nebula --port=3699 -u=user -p=password
```

to connect to the graph server. -->

连接 Nebula Graph：

```
> bin/nebula -u=user -p=password
```

在部署过程遇到的任何问题，欢迎你前往 [GitHub](https://github.com/vesoft-inc/nebula/issues) 向我们提 issue。

### 编译源码方式 (Linux)

**编译**

请参见 [how-to-build.md](./how-to-build.md)

#### 运行 Nebula

* 配置 nebula-metad.conf

   在 nebula 安装目录(/usr/local/nebula/)下，运行以下命令：

   ```
   > cp etc/nebula-metad.conf.default etc/nebula-metad.conf
   ```

   根据实际修改 nebula-metad.conf 中的配置：

   - local_ip: ip地址
   - port: 端口号
   - ws_http_port: metaservice HTTP服务端口号
   - ws_h2_port: metaservice HTTP2服务端口号
   - meta_server_addrs: meta server 地址列表，格式为 ip1:port1, ip2:port2, ip3:port3

* 配置 nebula-storaged.conf

   ```
   > cp etc/nebula-storaged.conf.default etc/nebula-storaged.conf
   ```

   根据实际修改 nebula-storaged.conf 中的配置：

   - local_ip: ip地址
   - port: 端口号
   - ws_http_port: storageservice HTTP 服务端口号
   - ws_h2_port: storageservice HTTP2 服务端口号
   - meta_server_addrs: meta server 地址列表，格式为 ip1:port1, ip2:port2, ip3:port3

* 配置 nebula-graphd.conf

   ```
   > cp etc/nebula-graphd.conf.default etc/nebula-graphd.conf
   ```
   根据实际修改 nebula-graphd.conf 中的配置：

   - port: 端口号
   - ws_http_port: graphservice HTTP 服务端口号
   - ws_h2_port: graphservice HTTP2 服务端口号
   - meta_server_addrs: meta server 地址列表，格式为 ip1:port1, ip2:port2, ip3:port3

**启动服务**

```
> scripts/nebula.service start all
```

查看服务状态

```
> scripts/nebula.service status all
```

**连接 Nebula Graph**

```
> bin/nebula -u=user -p=password
```

* -u 为用户名，默认值为 `user`
* -p 为密码，用户 `user` 的默认密码为 `password`

<!-- `ADD HOSTS` 将存储节点注册到元数据服务中

```
> ADD HOSTS $storage_ip:$storage_port
```

根据nebula-storaged.conf中的 `local_ip` 和 `port` 替换此处的 `$storage_ip` 和 `$storage_port`。 -->

---

## 创建图数据

本节介绍如何构建图数据并进行查询。本示例基于下图构建：

![Untitled Diagram (1)](https://user-images.githubusercontent.com/51590253/60649144-0774c980-9e74-11e9-86d6-bad1653e70ba.png)

示例数据有三种类型的标签（_course_， _building_，_student_），两种类型的边（_select_ 和 _like_），其 Schema 为：

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

Nebula 中的图存储于 **SPACE** 中，每个 Space 是一个物理隔离的空间。首先，需要创建一个 Space ，并使用该 Space 以完成之后的操作。

列出已有的 Space：

```
nebula> SHOW SPACES;
```

创建一个名为 myspace_test2 的新 Space：

```
nebula> CREATE SPACE myspace_test2(partition_num=1, replica_factor=1);

-- 使用这个 space
nebula> USE myspace_test2;
```

- `replica_factor` 用来指定集群副本数。

- `partition_num` 用来指定一个副本中的分区数量。

### 定义图数据 Schema

使用 `CREATE TAG` 语句定义带有标签类型和属性列表的标签：

```
nebula> CREATE TAG course(name string, credits int);
nebula> CREATE TAG building(name string);
nebula> CREATE TAG student(name string, age int, gender string);
```

使用 `CREATE EDGE` 语句定义边类型：

```
nebula> CREATE EDGE like(likeness double);
nebula> CREATE EDGE select(grade int);
```

查看上述创建的标签和边类型：

```
-- 查看标签列表
nebula> SHOW TAGS;

-- 查看边类型列表
nebula> SHOW EDGES;
```

查看标签或边类型的属性：

```
-- 查看标签的属性
nebula> DESCRIBE TAG student;

-- 查看边类型的属性
nebula> DESCRIBE EDGE like;
```


### 插入数据

根据上图，插入相应的点和边：

```
-- 插入点
nebula> INSERT VERTEX student(name, age, gender) VALUES 200:("Monica", 16, "female");
nebula> INSERT VERTEX student(name, age, gender) VALUES 201:("Mike", 18, "male");
nebula> INSERT VERTEX student(name, age, gender) VALUES 202:("Jane", 17, "female");
nebula> INSERT VERTEX course(name, credits),building(name) VALUES 101:("Math", 3, "No5");
nebula> INSERT VERTEX course(name, credits),building(name) VALUES 102:("English", 6, "No11");

-- 插入边
nebula> INSERT EDGE select(grade) VALUES 200 -> 101:(5);
nebula> INSERT EDGE select(grade) VALUES 200 -> 102:(3);
nebula> INSERT EDGE select(grade) VALUES 201 -> 102:(3);
nebula> INSERT EDGE select(grade) VALUES 202 -> 102:(3);
nebula> INSERT EDGE like(likeness) VALUES 200 -> 201:(92.5);
nebula> INSERT EDGE like(likeness) VALUES 201 -> 200:(85.6);
nebula> INSERT EDGE like(likeness) VALUES 201 -> 202:(93.2);
```

### 查询示例

Q1. 查询点 201 喜欢的点：

```
nebula> GO FROM 201 OVER like;

=============
| like._dst |
=============
| 200       |
-------------
| 202       |
-------------
```

Q2. 查询点 201 喜欢的点，并筛选出年龄大于 17 岁的点，并返回其姓名，年龄，性别，将其重全名为 Friend，Age，Gender。

```
nebula> GO FROM 201 OVER like WHERE $$.student.age >= 17 YIELD $$.student.name AS Friend, $$.student.age AS Age, $$.student.gender AS Gender;

=========================
| Friend | Age | Gender |
=========================
|   Jane |  17 | female |
-------------------------
```
说明:
- `YIELD` 用来指定返回信息。

- `$^` 为起始点。

- `$$` 为目标点。

Q3. 查询点201喜欢的点选择了哪些课程和其对应年级。

```
-- 使用管道
nebula> GO FROM 201 OVER like yield like._dst as id | GO FROM $-.id OVER select YIELD $^.student.name AS Student, $$.course.name AS Course, select.grade AS Grade;

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
nebula> $a=GO FROM 201 OVER like yield like._dst as id; GO FROM $a.id OVER select YIELD $^.student.name AS Student, $$.course.name AS Course, select.grade AS Grade;

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

说明:
- `|` 表示管道操作，前一个子查询的结果传递给后一个子查询。

- `$-` 表示输入流。

第二种方法使用了用户定义变量 `$a`，此变量可以在整个复合语句内使用。

<!-- 有关查询语言的更多详细信息，请查看check [nGQL Query Language](nGQL-tutorial.md)。 -->

## 常见问题

### graphd 的配置没有注册到 meta server

   用 `nebula.service` 脚本启动服务时，`graphd`、 `metad` 和 `storaged` 进程启动速度太快，可能会导致graphd 的配置没有注册到 meta server。restart 的时候也有此问题。
   beta版本用户可以先启动 metad，再启动 storaged 和 graphd 来避免此问题。我们将在下一个版本解决此问题。

   先启动 metad：

   ```
   nebula> scripts/nebula.service start metad
   [INFO] Starting nebula-metad...
   [INFO] Done
   ```

   再启动 storaged 和 graphd：

   ```
   nebula> scripts/nebula.service start storaged
   [INFO] Starting nebula-storaged...
   [INFO] Done

   nebula> scripts/nebula.service start graphd
   [INFO] Starting nebula-graphd...
   [INFO] Done
   ```

### 当创建 tag 或者 edge 类型后，插入数据时报错

可能原因, `load_data_interval_secs` 设置了从 meta server 获取元数据时间间隔，生产场景 (默认值) 是120s，s试用时可改为1s。更改方式:

- 启动前在nebula-metad.conf 和 nebula-graphd.conf 中加入

   ```
   --load_data_interval_secs=1
   ```

- console启动后，运行：

   ```
   nebula> UPDATE VARIABLES graph:load_data_interval_secs=1
   ```

### 使用docker启动后，执行命令时报错.

可能的原因是docker的IP地址和我们默认配置中的监听地址不一致(默认是172.17.0.2)，因此这里需要修改默认配置中的监听地址.

1. 首先在容器中执行ifconfig命令，查看您的容器地址，这里假设您的容器地址是172.17.0.3,那么就意味着您需要修改默认配置的IP地址.
2. 然后进入配置目录(cd /usr/local/nebula/etc), 查找所有IP地址配置的位置(grep "172.17.0.2" . -r).
3. 修改上一步查到的所有IP地址为您的容器地址(172.17.0.3).
4. 最后重新启动所有服务.

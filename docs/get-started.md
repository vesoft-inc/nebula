
---

This tutorial provides a quick introduction to use `Nebula Graph`.

---

##  Install Nebula Graph

### From Docker

The easiest way to startup `nebula` is using `docker`.

First of all, you should make sure that `docker` has been installed on your machine. Open a terminal and run the following command :

```
> docker --version
Docker version 18.09.2, build 6247962
```

If `docker` is not found, please see [here](https://docs.docker.com/install/) for more information to install docker.

After that, using
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
to get `nebula` docker images.


Then type command `docker images` to check the image status.

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

**Startup Nebula Graph**

When `nebula` image is ready, run

```
> docker run -it vesoft/nebula-graph:latest /bin/bash
```

to start and login to the docker container.
After login, you're in the `root` directory and you should use `cd ~/nebula-graph/` to switch to the nebula home directory.

Run

```
> ./start-all.sh
Starting MetaService StorageService and GraphService ...
```
to start meta service, storage service and graph service.

Run

```
> ps -ef | grep nebula
```

to display the services' running status.

Please make sure the services are working.

<!-- `bin/nebula` is a `console` which can be used to insert and query data.

`--port` is used for specifying the graph server port and the default value is `3699`.

`-u` and `-p` are used to specify the user name and password, `user` and `password` are the default authority.

Run

```
 > bin/nebula --port=3699 -u=user -p=password
```

to connect to the graph server. -->

To connect to the graph server, run

```
> ./start-console.sh

Welcome to Nebula Graph (Version 0.1)

nebula>
```

If you have any questions or concerns about the deployment procedures, please do not hesitate to open an issue on [GitHub](https://github.com/vesoft-inc/nebula/issues).

### From Source(Linux)

**Prerequisite Tools**

Nebula Graph is written in C++14, so it requires a complier supporting C++14 features.

3rd-party Libraries

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

It is recommended to install g++ 5 or higher Linux system, such as Fedora 29.
Currently, we are using `git-lfs` to store the 3rd-party libraries so make sure
`git-lfs` have been installed before fetching the source code.

**Fetch from GitHub**

```
> git clone https://github.com/vesoft-inc/nebula.git
```

**Compiling**

```
> cmake ./
```

The default installation is in /usr/local path. To specify the installation path,
use:

```
> cmake -DCMAKE_INSTALL_PREFIX=$your_nebula_install_dir
```

to replace the `$your_nebula_install_dir` here

Then run the following command:

```
> make && make install
```

**Running**

Configure nebula-metad.conf

In your Nebula installation directory, run

```
> cp etc/nebula-metad.conf.default etc/nebula-metad.conf
```

Modify configurations in nebula-metad.conf:

- local_ip
- port
- ws_http_port metaservice HTTP
- ws_h2_port metaservice HTTP2


Configure nebula-storaged.conf

```
> cp etc/nebula-storaged.conf.default etc/nebula-storaged.conf
```

Modify configurations in nebula-storaged.conf:

- local_ip
- port
- ws_http_port storageservice HTTP
- ws_h2_port storageservice HTTP2

Configure nebula-graphd.conf

```
> cp etc/nebula-graphd.conf.default etc/nebula-graphd.conf
```

Modify configurations in nebula-graphd.conf:

- local_ip
- port
- ws_http_port graphservice HTTP
- ws_h2_port graphservice HTTP2

**Start service**

```
> scripts/nebula.service start all
```

Make sure all the services are working

```
> scripts/nebula.service status all
```

**Connect to Nebula**

```
> bin/nebula -u=user -p=password
```

- -u is to set user name, `user` is the default Nebula user account
- -p is to set password, `password` is the default password for account `user`

`Add HOSTS` is to register the storage hosts:
```
> ADD HOSTS $storage_ip:$storage_port
```

Replace the `$storage_ip` and `$storage_port` here according to the `local_ip`
and `port` in nebula-storaged.conf

Then you’re now ready to start using Nebula Graph.

---

## Build Your Own Graph

This section describes how to build a graph and run queries. The example is built on the graph below:

![Untitled Diagram (1)](https://user-images.githubusercontent.com/51590253/60649144-0774c980-9e74-11e9-86d6-bad1653e70ba.png)

There are three kinds of tags (_course_, _building_ and _student_) and two edge types (_select_ and _like_). The graph schema is:
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


### Create a Graph Space

**SPACE** is a region that provides physically isolation of graphs in Nebula. First we need to create a space and use it before other operations.



To list all existing spaces:
```
nebula> SHOW SPACES;
```

To create a new space named myspace_test2 :
```
nebula> CREATE SPACE myspace_test2(partition_num=1, replica_factor=1);

-- Use this space
nebula> USE myspace_test2;
```
`replica_factor` specifies the number of replicas in the cluster.

`partition_num` specifies the number of partitions in one replica.

### Define Graph Schema

The `CREATE TAG` statement defines a tag, with a type name and an attribute list.
```
nebula> CREATE TAG course(name string, credits int);
nebula> CREATE TAG building(name string);
nebula> CREATE TAG student(name string, age int, gender string);
```
The `CREATE EDGE` statement defines an edge type.
```
nebula> CREATE EDGE like(likeness double);
nebula> CREATE EDGE select(grade int);
```

To list the tags and edge types that we just created：
```
-- Show tag list
nebula> SHOW TAGS;

-- Show edge type list
nebula> SHOW EDGES;
```

To show the attributes of a tag or an edge type:
```
-- Show attributes of a tag
nebula> DESCRIBE TAG student;

-- Show attributes of an edge type
nebula> DESCRIBE EDGE like;
```


### Insert Data


Insert the vertices and edges based on the graph above.
```

-- Insert vertices
nebula> INSERT VERTEX student(name, age, gender) VALUES 200:("Monica", 16, "female");
nebula> INSERT VERTEX student(name, age, gender) VALUES 201:("Mike", 18, "male");
nebula> INSERT VERTEX student(name, age, gender) VALUES 202:("Jane", 17, "female");
nebula> INSERT VERTEX course(name, credits),building(name) VALUES 101:("Math", 3, "No5");
nebula> INSERT VERTEX course(name, credits),building(name) VALUES 102:("English", 6, "No11");

```

```
-- Insert edges
nebula> INSERT EDGE select(grade) VALUES 200 -> 101:(5);
nebula> INSERT EDGE select(grade) VALUES 200 -> 102:(3);
nebula> INSERT EDGE select(grade) VALUES 201 -> 102:(3);
nebula> INSERT EDGE select(grade) VALUES 202 -> 102:(3);
nebula> INSERT EDGE like(likeness) VALUES 200 -> 201:(92.5);
nebula> INSERT EDGE like(likeness) VALUES 201 -> 200:(85.6);
nebula> INSERT EDGE like(likeness) VALUES 201 -> 202:(93.2);
```

## Sample Queries

Q1. Find the vertexes that 201 likes:

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

Q2. Find the vertexes that 201 likes, whose age are greater than 17. Return their name, age and gender, and alias the columns as Friend, Age and Gender, respectively.

```
nebula> GO FROM 201 OVER like WHERE $$.student.age >= 17 YIELD $$.student.name AS Friend, $$.student.age AS Age, $$.student.gender AS Gender;

=========================
| Friend | Age | Gender |
=========================
|   Jane |  17 | female |
-------------------------
```
`YIELD` specifies what values or results you might want to return from query.

`$^` represents the source vertex.

`$$` indicates the target vertex.

Q3. Find the selected courses and corresponding grades of students liked by 201.

```

-- By pipe
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

-- By temporary variable
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

`|` denotes a pipe. The output of the formal query acts as input to the next one like a pipeline.


`$-` refers to the input stream.

The second approach adopts a user-defined variable `$a`. The scope of this variable is within the compound statement.

For more details about Query Language, check [nGQL Query Language](nGQL-tutorial.md).

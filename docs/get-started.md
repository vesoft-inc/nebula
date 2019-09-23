
# Quick Start

##  Install Nebula Graph

### From Docker

The easiest way to get Nebula Graph up and running is using Docker. Before you start, make sure that you have:

* Installed the latest version of [Docker](https://docs.docker.com/)

* Pulled the latest images of Nebula from [Nebula Docker Hub](https://hub.docker.com/r/vesoft/nebula-graph). If not, pull the images using the following command:

```
> docker pull vesoft/nebula-graph:latest
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


**Startup Nebula Graph**

When `nebula` image is ready, run a container:

```
> docker run -it vesoft/nebula-graph:latest /bin/bash
```

After login, you're in the `root` directory and you should switch to the nebula directory

```
> cd /usr/local/nebula/
```

Start meta service, storage service and graph service:

```
> scripts/nebula.service start all
```

Check services' status:

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

**Connect to Nebula Graph**

connect to Nebula:

```
> bin/nebula -u=user -p=password
```

* -u is to set the user name, `user` is the default Nebula user account
* -p is to set password, `password` is the default password for account `user`

If you have any questions or concerns about the deployment procedures, please do not hesitate to open an issue on [GitHub](https://github.com/vesoft-inc/nebula/issues).

### From Source(Linux)

**Compiling**

Click [how to build](https://github.com/vesoft-inc/nebula/blob/master/docs/how-to-build.md) to compile **Nebula Graph**.


**Running**

* Configure nebula-metad.conf

   In your Nebula installation directory(/usr/local/nebula/), run

```
   > cp etc/nebula-metad.conf.default etc/nebula-metad.conf
```

   Modify configurations in nebula-metad.conf:

   - local_ip
   - port
   - ws_http_port: metaservice HTTP port
   - ws_h2_port: metaservice HTTP2 port
   - meta_server_addrs: List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3


* Configure nebula-storaged.conf

```
   > cp etc/nebula-storaged.conf.default etc/nebula-storaged.conf
```

   Modify configurations in nebula-storaged.conf:

   - port
   - ws_http_port: storageservice HTTP port
   - ws_h2_port: storageservice HTTP2 port
   - meta_server_addrs: List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3

* Configure nebula-graphd.conf

```
   > cp etc/nebula-graphd.conf.default etc/nebula-graphd.conf
```

   Modify configurations in nebula-graphd.conf:

   - local_ip
   - port
   - ws_http_port: graphservice HTTP port
   - ws_h2_port: graphservice HTTP2 port
   - meta_server_addrs: List of meta server addresses, the format looks like ip1:port1, ip2:port2, ip3:port3

**Start Service**

```
> scripts/nebula.service start all
```

Make sure all the services are working

```
> scripts/nebula.service status all
```

**Connect to Nebula Graph**

```
> bin/nebula -u=user -p=password
```

* -u is to set the user name, `user` is the default Nebula user account
* -p is to set password, `password` is the default password for account `user`

<!--
`Add HOSTS` is to register the storage hosts:

```
> ADD HOSTS $storage_ip:$storage_port
```

Replace the `$storage_ip` and `$storage_port` here according to the `local_ip`
and `port` in nebula-storaged.conf -->

Then you’re now ready to start using Nebula Graph.

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

**SPACE** is a region that provides physically isolation of graphs in Nebula. First, we need to create a space and use it before other operations.



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
* `replica_factor` specifies the number of replicas in the cluster.

* `partition_num` specifies the number of partitions in one replica.

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

-- Insert edges
nebula> INSERT EDGE select(grade) VALUES 200 -> 101:(5);
nebula> INSERT EDGE select(grade) VALUES 200 -> 102:(3);
nebula> INSERT EDGE select(grade) VALUES 201 -> 102:(3);
nebula> INSERT EDGE select(grade) VALUES 202 -> 102:(3);
nebula> INSERT EDGE like(likeness) VALUES 200 -> 201:(92.5);
nebula> INSERT EDGE like(likeness) VALUES 201 -> 200:(85.6);
nebula> INSERT EDGE like(likeness) VALUES 201 -> 202:(93.2);
```

### Sample Queries

Q1. Find the vertexes that 201 likes:

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

Q2. Find the vertexes that 201 likes, whose ages are greater than 17. Return their name, age and gender, and alias the columns as Friend, Age and Gender, respectively.

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

-- By temporary variable
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

`|` denotes a pipe. The output of the formal query acts as input to the next one like a pipeline.

`$-` refers to the input stream.

The second approach adopts a user-defined variable `$a`. The scope of this variable is within the compound statement.

For more details about Query Language, check [nGQL Query Language](nGQL-tutorial.md).

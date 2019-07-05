
---

This tutorial provides a quick introduction to use Nebula Graph.

---

### Step 1: Install Nebula Graph

The easiest way to startup Nebula is using Docker.
Docker is a tool designed to make it easier to create, deploy, and run applications by using containers.
Containers allow a developer to package up an application with all of the parts it needs,
such as libraries and other dependencies, and ship it all out as one package.
By doing so, the developer can rest assured that the application will run on any other Linux machine regardless of any customized settings that machine might have that could differ from the machine used for writing and testing the code.

First of all, you should make sure that Docker has been installed on your machine. Open a terminal and run the following command :

```
docker --version
```

If Docker is not found, please see [here](https://docs.docker.com/install/) for more information to install Docker.

After that, using
```
docker pull vesoft/nebula-graph:latest
```
to get `nebula` image and `docker images` that can display images status.

If the pulling is slow when pulling the image, configure a new mirror.
1. Check if the `/etc/docker` folder exists, if not, create a new one with `mkdir -p /etc/docker`. Generally the folder will exist after Docker installation.
2. Create the new file `daemon.json` with the following command

```
tee /etc/docker/daemon.json <<-'EOF'
{
  "registry-mirrors": [
    "https://dockerhub.azk8s.cn",
    "https://reg-mirror.qiniu.com"
  ]
}
EOF
```
Or create the new file with `vi /etc/docker/daemon.json`, then add the following content

```
{
  "registry-mirrors": [
    "https://dockerhub.azk8s.cn",
    "https://reg-mirror.qiniu.com"
  ]
}
```

---

### Step 2: Startup Nebula Graph

When `nebula` image is ready, run

```
docker run -it vesoft/nebula-graph:latest /bin/bash
```

to start and log in to the Docker container.
After login, you're in the `root` directory and you should use `cd ~/nebula-graph/` to switch to the nebula home directory.

Run
```
./start-all.sh
```
to start meta service, storage service and graph service.

Run

```
ps -ef | grep nebula
```

to display the services' running status.

Please make sure the services are working.

`bin/nebula` is a `console` which can be used to insert and query data.

`--port` is used for specifying the graph server port and the default value is `3699`.

`-u` and `-p` are used to specify the user name and password, `user` and `password` are the default authority.

Run

```
bin/nebula --port=3699 -u=user -p=password
```

to connect to the graph server.

One easier way to start console is to run

```
./start-console.sh
```

```
Welcome to Nebula Graph (Version 0.1)

nebula>
```

If you have any questions or concerns about the deployment procedures, please do not hesitate to open an issue on git.

### Step 3: Build Your Own Graph
This section describes how to build a graph and make queries. The example is built on the graph below.

![Untitled Diagram (1)](https://user-images.githubusercontent.com/51590253/60649144-0774c980-9e74-11e9-86d6-bad1653e70ba.png)

There are three kinds of tags(course, building and team) and two edge types (select and like). The graph schema is:
```json
{
     "tags": {
       "course": ["name: string", "credits: integer"],
       "building": ["name: string"],
       "student": ["name: string", "age: integer", "gender: string"]
     },
     "edges": {
        "select": ["grade: integer"],
        "like": ["likeness: double"]
     }
}
```

#### Create a Graph Space
<em>Space</em> is a region that provides physically isolation of graphs in Nebula. First we need to create a space and use it before other operations.

To list all existing spaces:
```shell
nebula> SHOW SPACES;
```

To create a new space named <em>myspace_test2</em>
```shell
nebula> CREATE SPACE myspace_test2(partition_num=1, replica_factor=1);

// use this space
nebula> USE myspace_test2;
```
<em>replica_factor</em> specifies the number of replicas in the cluster.

<em>partition_num</em> specifies the number of partitions in one replica.

#### Define Graph Schema
The **CREATE TAG** statement defines a tag, with a type name and an attribute list.
```shell
nebula> CREATE TAG course(name string, credits int);
nebula> CREATE TAG building(name string);
nebula> CREATE TAG student(name string, age int, gender string);
```
The **CREATE EDGE** statement defines an edge type.
```shell
nebula> CREATE EDGE like(likeness double);
nebula> CREATE EDGE select(grade int);
```

To list the tags and edge types we just created：
```shell
-- Show tag list
nebula> SHOW TAGS;

-- Show edge type list
nebula> SHOW EDGES;
```

To show the attributes of a tag or an edge type:
```shell
-- Show attributes of a tag
nebula> DESCRIBE TAG student;

-- Show attributes of an edge type 
nebula> DESCRIBE EDGE like;
```

#### Insert Data
Insert the vertexes and edges based on the graph above.
```shell
-- Insert vertexes
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

#### Sample Queries
Q1. Find the vertexes that 201 likes:
```shell
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

```shell
nebula> GO FROM 201 OVER like WHERE $$[student].age >= 17 YIELD $$[student].name AS Friend, $$[student].age AS Age, $$[student].gender AS Gender;

=========================
| Friend | Age | Gender |
=========================
|   Jane |  17 | female |
-------------------------
```
**YIELD** specifies what values or results you might want to return from query.

**$^** represents the source vertex.

**$$** indicates the target vertex.

Q3. Find the courses that the vetexes liked by 201 select and their grade.

```shell
-- By pipe
nebula> GO FROM 201 OVER like | GO FROM $-.id OVER select YIELD $^[student].name AS Student, $$[course].name AS Course, select.grade AS Grade;

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
nebula> $a=GO FROM 201 OVER like; GO FROM $a.id OVER select YIELD $^[student].name AS Student, $$[course].name AS Course, select.grade AS Grade;

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

Symbol ’**|**‘ denotes a pipe. The output of the formal query acts as input to the next one like a pipeline.

$- refers to the input stream.

The second approach adopts a user-defined variable. The scope of this variable is within the compound statement.

For more details about Query Language, check [nGQL](https://github.com/vesoft-inc/nebula/blob/master/docs/nGQL.md).
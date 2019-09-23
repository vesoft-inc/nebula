<!-- # 查询语言nGQL

nGQL是Nebula Graph的查询语言，可供用户存储和检索图数据。nGQL为类SQL式语言，易学易用，且可满足复杂业务需求。

## nGQL的关键属性

Nebula Graph致力于打造专为图数据使用的查询语言——nGQL。与其他图数据库查询语言相比，nGQL具有以下特点：

- 声明式：与命令式语言不同，nGQL是声明式语言，只需直接声明查询模式而无需告知如何查询。

- 表现力强：nGQL采用ASCII语法风格，其查询语言所表达的真实含义易于理解。

## nGQL语法

nGQL关键字不区分大小写，但是建议将其全部大写，以便于阅读。为帮助您快速了解nGQL，我们创建了一个名为`myspace_test`的简单的图数据，其中包含4个节点和3条边。

### 集群管理

* 添加hosts

添加一个host

```
ADD HOSTS $storage_ip:$storage_port
```

添加多个hosts

```
ADD HOSTS $storage_ip1:$storage_port1,
$storage_ip2:$storage_port2,...
```

**注意：**

将此处的$storage_ip和$storage_port替换为nebula-storaged.conf文件中的local_ip和port端口号，hosts之间需用逗号隔开，例如：

```
ADD HOSTS 192.168.8.5:65500
```


- 显示hosts

```
SHOW HOSTS
=============================
|          Ip |  Port | Status |
=============================
| 192.168.8.5 | 65500 | online |
-----------------------
| 192.168.8.1 | 65500 | offline |
-----------------------
```

- 移除hosts

移除一个host

```
REMOVE HOSTS $storage_ip:$storage_port
```

移除多个hosts

```
REMOVE HOSTS $storage_ip1:$storage_port1, $storage_ip2:$storage_port2,...
```

**注意：** 使用逗号将hosts隔开。

### 图管理

SPACE是物理隔离的空间，作用类似于MySQL中的数据库。

| | CREATE | DROP | USE | DESCRIBE | SHOW |
|---| --- | --- | ----- | -------- | ---- |
| SPACE | √ | √  | √    | v0.2     | √    |

增加CREATE、删除DROP、使用USE、列举SHOW，描述DESCRIBE。
举例如下：

* 列举出当前所有的space：

```
SHOW SPACES
================
|         Name |
================
| myspace_test |
----------------
```

* 删除space

```
DROP SPACE myspace_test
```
**注意：** 当前版本DROP SPACE后，其中的所有数据会同时删除，且尚未支持恢复功能

* 创建space

```
CREATE SPACE myspace_test(partition_num=10, replica_factor=1)
```

**注意：** partition_num用于控制数据分片数；replica_factor用于控制raft副本数量，单机设为1

* 指定使用space

```
USE myspace_test
```

### Schema管理

Schema用于管理节点和边的属性（每个字段的命名和类型），nebula中一个节点可被打卡多个标签。

|    | CREATE | DROP | ALTER | DESCRIBE | SHOW | TTL | LOAD | DUMP |
|:-: | :-: | :-: |:-: | :-: | :-: | :-: |:-: | :-: |
|TAG | √      | v0.2 |    v0.2  |      √   |   √  |  v0.3  | v0.2    |  v0.3   |
|EDGE| √      |v0.2  |  v0.2 |  √       |  √   |v0.3 | v0.2 | v0.3 |

CREATE,DROP,ALTER,DESCRIBE分别对应一个SCHEMA的新建、删除、修改和查看
举例如下：

```
CREATE TAG player(name string, age int);
```

```
DESCRIBE TAG player;
```

```
CREATE TAG team(name string);
```

```
DESCRIBE TAG team;
```

```
CREATE EDGE serve (start_year int, end_year int);
```

```
DESCRIBE EDGE serve;
```

```
CREATE EDGE like (likeness double);
```

```
SHOW TAGS;
```

```
SHOW EDGES
```

### 数据操作

INSERT 用于插入新的节点或边，更新和删除操作会在v0.2一起发布。

|   | INSERT | UPDATE | REMOVE |
|:-: | :-: | :-: |:-: |
|TAG | √   | v0.2     | v0.2   |
|EDGE | √   | v0.2    | v0.2|

插入节点时，需要指定节点的标签类型，以及节点id（也可通过hash自动生成）。

举例如下：

```
INSERT VERTEX player(name, age) VALUES 100:("Stoudemire", 36); -- 手动指定节点ID
```

```
INSERT VERTEX player(name, age) VALUES hash("Jummy"):("Jummy", 0);  -- hash生成节点ID
```

```
INSERT VERTEX player(name, age) VALUES 101:("Vicenta", 0);
```

```
INSERT VERTEX team(name) VALUES 201:("Magic");
```

```
INSERT EDGE like (likeness) VALUES 100 -> 101:(90.02);
```

```
INSERT EDGE like (likeness) VALUES 101 -> 102:(10.00);
```

```
INSERT EDGE serve (start_year, end_year) VALUES 101 -> 201:(2002, 2010);
```

### 图查询

当前最常用的图查询/遍历算子是GO，语义是从某个点开始，查询1度近邻。通过结合管道`|`，过滤WHERE、YIELD等条件，实现多跳复杂查询。举例如下：

```
GO FROM 100 OVER like; -- 从点100开始，沿like类型的边查询1跳
```

```
GO 2 STEPS FROM 100 OVER like; -- 从点100开始，沿like类型的边查询2跳
```

```
GO FROM 100 OVER like WHERE likeness >= 0; -- 从点100开始，沿like类型的边，过滤边上属性likeness
```

```
GO FROM 100 OVER like WHERE $$.player.name=="Vicenta"; -- 过滤要求：终点节点name字段为“Vicenta”
```

```
GO FROM 101 OVER serve YIELD serve._src AS src_id, $^.player.age AS src_propAge, serve._dst AS dst_id, $$.team.name AS dst_propName; -- 返回起点id（重命名为srcid），起点属性age，终点id，终点属性name
```

```
GO FROM 100 OVER like | GO FROM $-.id OVER serve; -- 从点100开始1跳，其输出作为下个query的输入（管道）
```



<!-- ## Syntax norms

In order to be consistent with ourselves and other nGQL users, we recommend
you to follow these syntax norms:

- KEYWORDS are in uppercase

  - eg: `SHOW SPACES` the keywords here are all written in uppercase

- Tags are in upper camel case (start with uppercase）

  - eg: `CREATE TAG ManageTeam` the tag name **ManageTeam** is written in upper
  camel case

- EDGES are in upper snake case (like IS_A)

  - eg: CREATE EDGE Play_for (name) the edge name **Play_for** is written in upper
   snake case

- Property names are in lower camel case

  - eg: inService


  | Graph entity  | Recommended style | Example |
  |:-: | :-: | :-: |:-: |
  |Key words | Upper case   | SHOW SPACES     |
  |Vertex tags | Upper camel case, beginning with an upper-case character   | ManageTeam   |
  |Edges | Upper snake case, beginning with an upper-case character   | Play_for   |
  |Property names | Lower camel case, beginning with a lower-case character   | inService   | --> -->

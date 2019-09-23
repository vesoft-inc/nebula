<!-- # nGQL Query Language -->

nGQL is the query language of Nebula Graph that allows users to store and retrieve
data from the graph database. Nebula Graph wants to make its queries easy to learn,
understand, and use for everyone.

## Key attributes of nGQL

Nebula Graph is committed to create a new query language that specifically deals
with graph data. nGQL has two attributes that are not available together in any
other query language out there.

- Declarative: nGQL is a declarative query language, which is very different from
 the imperative alternatives out there. You declare the pattern that you are looking for. You effectively tell nGQL what you want, rather than how to get it.

- Expressive: nGQL's ASCII-art style syntax provides a familiar, readable way to
 match patterns of nodes and relationships within graph datasets.

## nGQL Syntax

nGQL key words are case-insensitive but we recommend them written in all caps for easy reading. To help you get a quick understanding of nGQL, we have created a simple graph `myspace_test` with 4 vertices and 3 edges.

### Cluster administration

* Add hosts

Add a single host

```
ADD HOSTS $storage_ip:$storage_port
```

Add multiple hosts

```
ADD HOSTS $storage_ip1:$storage_port1,
$storage_ip2:$storage_port2,...
```

**Note:**
Replace the $storage_ip and $storage_port here according to the local_ip and port in nebula-storaged.conf. Separate the hosts by comma. For example:

```
ADD HOSTS 192.168.8.5:65500
```


- Show hosts

```
SHOW HOSTS
=================================
| Ip          | Port  | Status  |
=================================
| 192.168.8.5 | 65500 | online  |
---------------------------------
| 192.168.8.1 | 65500 | offline |
---------------------------------
```

* Remove hosts

Remove a single host

```
REMOVE HOSTS $storage_ip:$storage_port
```

Remove multiple hosts

```
REMOVE HOSTS $storage_ip1:$storage_port1, $storage_ip2:$storage_port2,...
```

**Note:** Separate the hosts by comma.

### Graph administration

Graph spaces are physically isolated like the database in MySQL.

|       | CREATE | DROP | USE   | DESCRIBE | SHOW | SHOW CREATE |
|---    | ---    | ---  | ----- | -------- | ---- | ----------- |
| SPACE | √      | √    | √     | √        | √    | √           |

Create space with CREATE, drop space with DROP, choose which space to use with USE, list available spaces with SHOW. DESCRIBE will be released in v0.2.

Following are some examples:

* List all the spaces available

```
SHOW SPACES
================
| Name         |
================
| myspace_test |
----------------
```

```
SHOW CREATE SPACES myspace_test
====================================================================================
| Space        | Create Space                                                      |
====================================================================================
| myspace_test | CREATE SPACE myspace_test (partition_num = 1, replica_factor = 1) |
------------------------------------------------------------------------------------
```

* Drop a space

```
DROP SPACE myspace_test
```
**Note:** DROP SPACE deletes all data in the current version and recovery is not supported yet.

* Create a space

```
CREATE SPACE myspace_test(partition_num=10, replica_factor=1)
```

**Note:** partition_num is used to control the number of shardings and replica_factor to control the number of raft copies, which is set to 1 when in stand-alone version.

* Specify space

```
USE myspace_test
```

### Schema mutation

Schema is used to manage the properties of vertices and edges (name and type of each field). In Nebula, a vertex can be labeled by multiple tags.

|    | CREATE | DROP | ALTER | DESCRIBE | SHOW | TTL  | LOAD | DUMP | SHOW CREATE |
|:-: | :-:    | :-:  |:-:    | :-:      | :-:  | :-:  | :-:  | :-:  | :-:         |
|TAG | √      | √    | √     |  √       | √    | ×    | √    | ×    | √           |
|EDGE| √      | √    | √     |  √       | √    | ×    | √    | ×    | √           |

You can use CREATE, DROP, ALTER, DESCRIBE, SHOW CREATE to create, drop, alter, view a schema.
Following are some examples:

```
CREATE TAG player(name string, age int);
```

```
DESCRIBE TAG player;
==================
| Field | Type   |
==================
| name  | string |
------------------
| age   | int    |
------------------
```

```
SHOW CREATE TAG player;
==========================================================================================
| Tag    | Create Tag                                                                    |
==========================================================================================
| player | CREATE TAG player (
  name string,
  age int
) ttl_duration = 0, ttl_col = "" |
------------------------------------------------------------------------------------------
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
=====================
| Field      | Type |
=====================
| start_year | int  |
---------------------
| end_year   | int  |
---------------------
```

```
SHOW CREATE EDGE serve;
=================================================================================================
| Edge  | Create Edge                                                                           |
=================================================================================================
| serve | CREATE EDGE serve (
  start_year int,
  end_year int
) ttl_duration = 0, ttl_col = "" |
-------------------------------------------------------------------------------------------------
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

### Data manipulation

INSERT is used to insert new vertices and edges, UPDATE AND REMOVE will be available in v0.2.

|     | INSERT | UPDATE | REMOVE |
|:-:  | :-:    | :-:    |:-:     |
|TAG  | √      | ×      | ×      |
|EDGE | √      | ×      | ×      |

When inserting a vertex, its tag type and attribute fields should be specified, while its ID can either be auto-generated by hash or specified manually.

Following are some examples:

```
INSERT VERTEX player(name, age) VALUES 100:("Stoudemire", 36); -- specify ID manually
```

```
INSERT VERTEX player(name, age) VALUES hash("Jummy"):("Jummy", 0);  -- ID generated by hash
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

### Graph query

The most commonly used graph query/traversal operator is GO, it means starting from a certain point and querying its 1 degree neighbor. Complex queries can be done by combining pipe `|`, filtering `WHERE`, `YIELD`, etc.

Following are some examples:

```
GO FROM 100 OVER like; -- Start from vertex 100, query 1-hop along edge like.
```

```
GO 2 STEPS FROM 100 OVER like; -- Start from vertex 100, query 2-hop along edge
```

```
GO FROM 100 OVER like WHERE likeness >= 0; -- Start from vertex 100, query along edge like and filter its property likeness
```

```
GO FROM 100 OVER like WHERE $$.player.name=="Vicenta"; -- Filter requirement: the destination vertex name is "Vicenta"
```

```
GO FROM 101 OVER serve YIELD serve._src AS src_id, $^.player.age AS src_propAge, serve._dst AS dst_id, $$.team.name AS dst_propName; -- Return the starting vertex id(renamed as srcid), source vertex property age, destination vertex id and its name
```

```
GO FROM 100 OVER like | GO FROM $-.id OVER serve; -- Start from vertex 100, query 1-hop, set its output as the next query's input by using pipe
```
<!-- ## Syntax norms -->

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


| Graph entity  | Recommended style                                          | Example     |
|:-:            | :-:                                                        | :-:         |
|Key words      | Upper case                                                 | SHOW SPACES |
|Vertex tags    | Upper camel case, beginning with an upper-case character   | ManageTeam  |
|Edges          | Upper snake case, beginning with an upper-case character   | Play_for    |
|Property names | Lower camel case, beginning with a lower-case character    | inService   |


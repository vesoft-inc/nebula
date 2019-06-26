# nGQL Query Language


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

nGQL key words are case-insensitive but we recommend them written in all caps for easy reading. Based on the released Nebula v0.1 and corresponding Docker mirror, we have created a simple graph with 4 vertices and 3 edges to help you get a quick understanding of nGQL.

### Graph space administration

Graph spaces are physically isolated like the database in MySQL.

| | CREATE | DROP | USE | DESCRIBE | SHOW |
|---| --- | --- | ----- | -------- | ---- |
| SPACE | √ | √  | √    | v0.2     | √    |

Create space with CREATE, drop space with DROP, choose which space to use with USE, list available spaces with SHOW. DESCRIBE will be released in v0.2.

Following are some examples:

* List all the spaces available

```
SHOW SPACES
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

|    | CREATE | DROP | ALTER | DESCRIBE | SHOW | TTL | LOAD | DUMP |
|:-: | :-: | :-: |:-: | :-: | :-: | :-: |:-: | :-: |
|TAG | √      | v0.2 |    v0.2  |      √   |   √  |  v0.3  | v0.2    |  v0.3   |
|EDGE| √      |v0.2  |  v0.2 |  √       |  √   |v0.3 | v0.2 | v0.3 |

You can use CREATE, DROP, ALTER, DESCRIBE to create, drop, alter, view a schema.
Following are some examples:

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
CREATE EDGE like (likeness double)；
```

```
SHOW TAGS;
```

```
SHOW EDGES
```

### Data manipulation

INSERT is used to insert new vertices and edges, UPDATE AND REMOVE will be available in v0.2.

|   | INSERT | UPDATE | REMOVE |
|:-: | :-: | :-: |:-: |
|TAG | √   | v0.2     | v0.2   |
|EDGE | √   | v0.2    | v0.2|

When inserting a vertex, its tag type and ID should be specified, but the attribute field can be ignored(the ignored fields are set to default values).

Following are some examples:

```
INSERT VERTEX player(name, age) VALUES 100:("Stoudemire", 36);
```

```
INSERT VERTEX player(name) VALUES 101:("Vicenta"); -- age is set to default value 0
```

```
INSERT VERTEX player(name) VALUES 102:("jummy");
```

```
INSERT VERTEX team(name) VALUES 201:("Magic");
```

```
INSERT EDGE like (likeness) VALUES 100 -> 101:(90.02);
```

```
INSERT EDGE like (likeness) VALUES 101 -> 102:(10);
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
GO FROM 100 OVER like WHERE $$[player].name=="Vicenta"; -- Filter requirement: the destination vertex name is "Vicenta"
```

```
GO FROM 101 OVER serve YIELD serve._src AS srcid, $^[player].age AS src.propAge, serve._dst AS dstid, $$[team].name AS dst.propName; -- Return the starting vertex id(renamed as srcid), source vertex property age, destination vertex id and its name
```

```
GO FROM 100 OVER like | GO FROM $-.id OVER serve; -- Start from vertex 100, query 1-hop, set its output as the next query's input by using pipe
```



## Syntax norms

In order to be consistent with ourselves and other nGQL users, we advise
you to follow these syntax norms:

- KEYWORDS are in uppercase

  - eg: `SHOW SPACES` the keywords here are all written in uppercase

- node Aliases are in lower camel case (start with lowercase)

  - eg:  node alias userName

- Tags are in upper camel case (start with uppercase）

  - eg: `CREATE TAG ManageTeam` the tag name **ManageTeam** is written in upper
  camel case

- EDGES are in upper snake case (like IS_A)

  - eg: CREATE EDGE Play_for (name) the edge name **Play_for** is written in upper
   snake case

- Property names are in lower camel case

  - eg: kobeBryant

# Schema Index

```ngql
CREATE {TAG | EDGE} INDEX [IF NOT EXISTS] <index_name> ON {<tag_name> | <edge_name>} (prop_name_list)
```

Schema indexes are built to fast process graph queries. **Nebula Graph** supports two different kinds of indexing to speed up query processing: **tag indexes** and **edge type indexes**.

Most graph queries start the traversal from a list of vertices or edges that are identified by their properties. Schema indexes make these global retrieval operations efficient on large graphs.

Normally, you create indexes on a tag/edge-type at the time the tag/edge-type itself is created with `CREATE TAG/EDGE` statement.

## Create Index

`CREATE INDEX` enables you to add indexes to existing tag/edge-type.

### Create Single-Property Index

```ngql
nebula> CREATE TAG INDEX player_index_0 on player(name);
```

The above statement creates an index for the _name_ property on all vertices carrying the _player_ tag.

```ngql
nebula> CREATE EDGE INDEX follow_index_0 on follow(degree);
```

The above statement creates an index for the _degree_ property on all edges carrying the _follow_ edge type.

### Create Composite Index

The schema indexes also support spawning over multiple properties. An index on multiple properties for all vertices that have a particular tag is called a composite index. Consider the following example:

```ngql
nebula> CREATE TAG INDEX player_index_1 on player(name,age);
```

This statement creates a composite index for the _name_ and _age_ property on all vertices carrying the _player_ tag.

<!-- Queries do no longer have to explicitly use an index, it’s more the behavior we know from SQL. When there is an index that can make a query more performant. Assume a query like

```ngql
MATCH (p:Person {name: 'Stefan'}) RETURN p
```

In case of no index being set up this will look up all Person nodes and check if their name property matches Stefan. If an index is present it will be used transparently. -->

## Show Index

```ngql
SHOW {TAG | EDGE} INDEXES
```

`SHOW INDEXES` returns the defined tag/edg-type index information. For example, list the indexes with the following command:

```ngql
nebula> SHOW TAG INDEXES;
=============================
| Index ID | Index Name     |
=============================
| 22       | player_index_0 |
-----------------------------
| 23       | player_index_1 |
-----------------------------

nebula> SHOW EDGE INDEXES;
=============================
| Index ID | Index Name     |
=============================
| 24       | follow_index_0 |
-----------------------------

```

## DESCRIBE INDEX

```ngql
DESCRIBE {TAG | EDGE} INDEX <index_name>
```

`DESCRIBE INDEX` is used to obtain information about the index. For example, list the index information with the following command:

```ngql
nebula> DESCRIBE TAG INDEX player_index_0;
==================
| Field | Type   |
==================
| name  | string |
------------------

nebula> DESCRIBE TAG INDEX player_index_1;
==================
| Field | Type   |
==================
| name  | string |
------------------
| age   | int    |
------------------
```

## DROP INDEX

```ngql
DROP {TAG | EDGE} INDEX [IF EXISTS] <index_name>
```

`DROP INDEX` drops the index named _index_name_ from the tag/edge-type. For example, drop the index _player_index_0_ with the following command:

```ngql
nebula> DROP TAG INDEX player_index_0;
```

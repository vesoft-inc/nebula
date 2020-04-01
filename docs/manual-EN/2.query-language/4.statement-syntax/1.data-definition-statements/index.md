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

The schema indexes also support spawning over multiple properties. An index on multiple properties is called a composite index.

**Note:** Index across multiple tags is not yet supported.

Consider the following example:

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

## REBUILD INDEX

```ngql
REBUILD {TAG | EDGE} INDEX <index_name> [OFFLINE]
```

[Create Index](#create-index) section describes how to build indexes to improve query performance. If the index is created before inserting the data, there is no need to rebuild index and this section can be skipped; if data is updated or newly inserted after the index creation, it is necessary to rebuild the indexes in order to ensure that the indexes contain the previously added data. If the current database does not provide any services, use the `OFFLINE` keyword to speed up the rebuilding.

<!-- > During the rebuilding, any idempotent queries will skip the index and perform sequential scans. This means that queries run slower during this operation. Non-idempotent commands, such as INSERT, UPDATE, and DELETE are blocked until the indexes are rebuilt. -->

After rebuilding is complete, you can use the `SHOW {TAG | EDGE} INDEX STATUS` command to check if the index is successfully rebuilt. For example:

```ngql
nebula> CREATE TAG person(name string, age int, gender string, email string);
Execution succeeded (Time spent: 10.051/11.397 ms)

nebula> CREATE TAG INDEX single_person_index ON person(name);
Execution succeeded (Time spent: 2.168/3.379 ms)

nebula> REBUILD TAG INDEX single_person_index OFFLINE;
Execution succeeded (Time spent: 2.352/3.568 ms)

nebula> SHOW TAG INDEX STATUS;
==========================================
| Name                | Tag Index Status |
==========================================
| single_person_index | SUCCEEDED        |
------------------------------------------
```

## Using Index

After the index is created and data is inserted, you can use the [LOOKUP](../2.data-query-and-manipulation-statements/lookup-syntax.md) statement to query the data.

There is usually no need to specify which indexes to use in a query, **Nebula Graph** will figure that out by itself.

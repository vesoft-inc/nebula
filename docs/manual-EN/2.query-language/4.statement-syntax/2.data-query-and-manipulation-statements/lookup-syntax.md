# Lookup Syntax

The `LOOKUP` statement is used to search for the filter condition in it. `LOOKUP` is often coupled with a `WHERE` clause which adds filters or predicates.

**Note:** Before using the `LOOKUP` statement, please make sure that indexes are created. Read more about indexes in [Index Documentation](../1.data-definition-statements/index.md).

```ngql
LOOKUP ON {<vertex_tag> | <edge_type>} WHERE <expression> [ AND | OR expression ...]) ] [YIELD <return_list>]

<return_list>
    <col_name> [AS <col_alias>] [, <col_name> [AS <col_alias>] ...]
```

- `LOOKUP` clause finds the vertices or edges.
- `WHERE` extracts only those results that fulfill the specified conditions. The logical AND, OR, NOT are also supported. See [WHERE Syntax](where-syntax.md) for more information.
  **Note:** `WHERE` clause does not support the following operations in `LOOKUP`:
  - `$-` and `$^`
  - In relational expressions, expressions with field-names on both sides of the operator are not currently supported, such as (tagName.column1> tagName.column2)

- `YIELD` clause returns particular results. If not specified, vertex ID is returned when `LOOKUP` tags, source vertex ID, dest vertex ID and ranking of the edges are returned when `LOOKUP` edges.

## Retrieve Vertices

The following example returns vertices whose name is `Tony Parker` and tagged with _player_.

```ngql
nebula> CREATE TAG INDEX index_player ON player(name, age);

nebula> LOOKUP ON player WHERE player.name == "Tony Parker";
============
| VertexID |
============
| 101      |
------------

nebula > LOOKUP ON player WHERE player.name == "Tony Parker" \
YIELD person.name, person.age;
=======================================
| VertexID | player.name | player.age |
=======================================
| 101      | Tony Parker | 36         |
---------------------------------------
```

## Retrieve Edges

The following example returns edges whose `degree` is 90 and the edge type is _follow_.

```ngql
nebula> CREATE EDGE INDEX index_follow ON follow(degree);

nebula> LOOKUP ON follow WHERE follow.degree == 90;
=============================
| SrcVID | DstVID | Ranking |
=============================
| 100    | 106    | 0       |
-----------------------------

nebula> LOOKUP ON follow WHERE follow.degree == 90 YIELD follow.degree;
=============================================
| SrcVID | DstVID | Ranking | follow.degree |
=============================================
| 100    | 106    | 0       | 90            |
---------------------------------------------
```

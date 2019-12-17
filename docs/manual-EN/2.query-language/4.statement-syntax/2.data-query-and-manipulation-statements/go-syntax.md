# Go Syntax

`GO` statement is the MOST commonly used clause in **Nebula Graph**.

It indicates to travel in a graph with specific filters (the `WHERE` clause), to fetch nodes and edges properties, and return results (the `YIELD` clause) with given order (the `ORDER BY ASC | DESC` clause) and numbers (the `LIMIT` clause).

> The syntax of `GO` statement is very similar to `SELECT` in SQL. Notice that the major difference is that `GO` must start traversing from a (set of) node(s).
<!-- >You can refer to `FIND` statement (in progress), which is the counterpart of `SELECT` in SQL. -->

```ngql
  GO [ <N> STEPS ] FROM <node_list>
  OVER <edge_type_list> [REVERSELY]
  [ WHERE <expression> [ AND | OR expression ...]) ]
  YIELD | YIELDS [DISTINCT] <return_list>

<node_list>
   | <vid> [, <vid> ...]
   | $-.id

<edge_type_list>
   edge_type [, edge_type ...]
   * # `*` will select all the available edge types

<return_list>
    <col_name> [AS <col_alias>] [, <col_name> [AS <col_alias>] ...]
```

* [ <N> STEPS ] specifies the N query hops
* <node_list> is either a list of node's vid separated by comma(,), or a special place holder `$-.id` (refer `PIPE` syntax).
* <edge_type_list> is a list of edge types which graph traversal can go through.
* [ WHERE <expression> ] extracts only those results that fulfill the specified conditions. WHERE syntax can be conditions for src-vertex, the edges, and dst-vertex. The logical AND, OR, NOT are also supported. see WHERE-syntax for more information.
* YIELD [DISTINCT] <return_list> statement returns the result in column format and rename as an alias name. See `YIELD`-syntax for more information. The `DISTINCT` syntax works the same as SQL.

## Examples

```ngql
nebula> GO FROM 101 OVER serve  \
   /* start from vertex 101 along with edge type serve, and get vertex 204, 215 */
=======
| id  |
=======
| 204 |
-------
| 215 |
-------
```

```ngql
nebula> GO 2 STEPS FROM 103 OVER follow \
  /* Returns the 2 hop friends of the vertex 103 */
===============
| follow._dst |
===============
| 100         |
---------------
| 101         |
---------------
```

```ngql
nebula> GO FROM 101 OVER serve  \
   WHERE serve.start_year > 1990       /* check edge (serve) property ( start_year) */ \
   YIELD $$.team.name AS team_name   /* target vertex (team) property serve.start_year */
================================
| team_name | serve.start_year |
================================
| Spurs     | 1999             |
--------------------------------
| Hornets   | 2018             |
--------------------------------
```

```ngql
nebula> GO FROM 100,102 OVER serve           \
        WHERE serve.start_year > 1995             /* check edge property */ \
        YIELD DISTINCT $$.team.name AS team_name, /* DISTINCT as SQL */ \
        serve.start_year,                         /* edge property */ \
        $^.player.name AS player_name             /* source vertex (player) property */
========================================================
| team_name     | serve.start_year | player_name       |
========================================================
| Trail Blazers | 2006             | LaMarcus Aldridge |
--------------------------------------------------------
| Spurs         | 2015             | LaMarcus Aldridge |
--------------------------------------------------------
| Spurs         | 1997             | Tim Duncan        |
--------------------------------------------------------
```

### Traverse Along Multiple Edges Types

Currently, **Nebula Graph** also supports traversing via multiple edge types with `GO`, the syntax is:

```ngql
GO FROM <node_list> OVER <edge_type_list | *> YIELD | YIELDS [DISTINCT] <return_list>
```

For example:

```ngql
nebula> GO OVER FROM <node_list> edge1, edge2....  // traverse alone edge1 and edge2 or
nebula> GO OVER FROM <node_list> *   // * means traverse along all edge types
```

> Please note that when traversing along multiple edges, there are some special restrictions on the use of filters(namely the `WHERE` statement), for example filters like `WHERE edge1.prop1 > edge2.prop2` is not supported.

As for return results, if multiple edge properties are to be returned, **Nebula Graph** will place them in different rows. For example:

```ngql
nebula> GO FROM 100 OVER edge1, edge2 YIELD edge1.prop1, edge2.prop2
```

 If vertex 100 has three edges in edge 1, two edges in edge 2, the final results is five rows as follows:

| edge1._prop1 | edge2._prop2 |
| --- | --- |
| 10 | "" |
| 20 | "" |
| 30 | "" |
| 0 | "nebula" |
| 0 | "vesoft" |

If there is no properties, the default value will be placed. The default value for numeric type is 0, and that for string type is an empty string, for bool is false, for timestamp is 0 (namely “1970-01-01 00:00:00”) and for double is 0.0.

Of course, you can query without specifying `YIELD`, which will return the vids of the dest vertices of each edge. Again, default values (here is 0) will be placed if there is no properties. For example, query `GO FROM 100 OVER edge1, edge2` returns the follow lines:

| edge1._dst | edge2._dst |
| --- | --- |
| 101 | 0 |
| 102 | 0 |
| 103 | 0 |
| 0 | 201 |
| 0 | 202 |

For query `GO FROM 100 OVER *`, the result is similar to the above example: the non-existing property or vid is populated with default values.
Please note that we can't tell which row belongs to which edge in the results. The future version will show the edge type in the result.

## Traverse Reversely

Currently, **Nebula Graph** supports traversing reversely using keyword `REVERSELY`, the syntax is:

```ngql
  GO FROM <node_list>
  OVER <edge_type_list> REVERSELY
  WHERE (expression [ AND | OR expression ...])  
  YIELD | YIELDS  [DISTINCT] <return_list>
```

For example:

```ngql
nebula> GO FROM 125 OVER follow REVERSELY YIELD follow._src AS id | \
        GO FROM $-.id OVER serve WHERE $^.player.age > 35 YIELD $^.player.name AS FriendOf, $$.team.name AS Team

=========================
| FriendOf    | Team    |
=========================
| Tim Duncan  | Spurs   |
-------------------------
| Tony Parker | Spurs   |
-------------------------
| Tony Parker | Hornets |
-------------------------
```

The above query first traverses players that follow player 125 and finds the teams they serve, then filter players who are older than 35, and finally it returns their names and teams. Of course, you can query without specifying `YIELD`, which will return the `vids` of the dest vertices of each edge by default.

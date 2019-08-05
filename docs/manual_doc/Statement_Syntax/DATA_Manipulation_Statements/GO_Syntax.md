`GO` statement is the MOST commonly used clause in Nebula. 

It indicates to travel in a graph with specific filter conditions (the `WHERE` clause), to fetch nodes and edges properties, and return results (the `YIELD` clause) with given order (the `ORDER BY ASC | DESC` clause) and numbers (the `LIMIT` clause).

>The syntax of `GO` statement (and `FIND` statement) is very similar to `SELECT` in SQL. Notice that the major difference is that `GO` must start traversing from a (set of) node(s)

>You can refer to `FIND/MATCH` statement (in progress), which is the counterpart of `SELECT` in SQL.

```
GO FROM <node_list> 
OVER <edge_type_list> 
WHERE (expression [ AND | OR expression ...])  
YIELD | YIELDS  [DISTINCT] <return_list>

<node_list>
   | vid [, vid ...]
   | $-.id
   
<edge_type_list>
   edge_type [, edge_type ...]

<return_list>   
    <col_name> [AS <col_alias>] [, <col_name> [AS <col_alias>] ...]
```

* <node_list> is either a list of node's vid separated by comma(,), or a special place holder `$-.id` (see `PIPE` syntax).
* <edge_type_list> is a list of edge types which graph traversal can go through.
* WHERE <filter_list> specify the logical conditions that must satisfy to be selected. WHERE-syntax can be conditions for src-vertex, the edges, and dst-vertex. The logical AND, OR, NOT are also supported. see WHERE-syntax for more information.
* YIELD [DISTINCT] <return_list> statement returns the result in column format and rename as an alias name. see `YIELD`-syntax for more information. The `DISTINCT`-syntax works the same as SQL.

### Examples

```
(user@127.0.0.1) [(none)]> GO FROM 101 OVER serve  \
   /* start from vertex 101 along with edge type serve, and get vertex 204, 215 */
=======
| id  |
=======
| 204 |
-------
| 215 |
-------
```


```
(user@127.0.0.1) [(none)]> GO FROM 101 OVER serve  \
   WHERE serve.start_year > 1990       /* check edge (serve) property ( start_year) */ \ 
   YIELD $$.team.name AS team_name,    /* target vertex (team) property serve.start_year */
================================
| team_name | serve.start_year |
================================
| Spurs     | 1999             |
-------------------------------- 
| Hornets   | 2018             | 
--------------------------------   
```

```
(user@127.0.0.1) [(none)]> GO FROM 100,102 OVER serve           \
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


### Reference

 

# Find Path Syntax

`FIND PATH` statement can be used to get the shortest path and all paths.

```ngql
FIND SHORTEST | ALL PATH FROM <vertex_id_list> TO <vertex_id_list> OVER <edge_type_list> [UPTO <N> STEPS]
```

`SHORTEST` is the keyword to find the shortest path.

`ALL` is the keyword to find all paths.

`<vertex_id_list>::=[vertex_id [, vertex_id]]` is the vertex id list,multiple ids should be separated with commas, and ```$-```and ```$var``` are supported.

`<edge_type_list>` is the edge type list, multiple edge types should be separated with commas, and ```*``` can be referred as all edge types.

`<N>` is hop number, and the default value is 5.

## Note

- When source and dest vertices are id lists, it means to find the shortest path from any source vertices to the dest vertices.
- There may be cycles when searching all paths.

## Examples

Path is displayed as `id <edge_name, ranking> id` in console.

```ngql
nebula> FIND SHORTEST PATH FROM 100 to 200 OVER *;
=============================
| _path_ |
=============================
| 100 <serve,0> 200
-----------------------------
```

```ngql
nebula>FIND ALL PATH FROM 100 to 200 OVER *;
=============================================================================================================
| _path_ |
=============================================================================================================
| 100 < serve,0> 200
-------------------------------------------------------------------------------------------------------------
| 100 <follow,0> 101 < serve,0> 200
-------------------------------------------------------------------------------------------------------------
| 100 <follow,0> 102 < serve,0> 200
-------------------------------------------------------------------------------------------------------------
| 100 <follow,0> 106 < serve,0> 200
-------------------------------------------------------------------------------------------------------------
```

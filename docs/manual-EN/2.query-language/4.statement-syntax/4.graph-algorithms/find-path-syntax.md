# Find Path Syntax

`FIND PATH` statement can be used to get the shortest path and the full path.

```
FIND SHORTEST | ALL PATH FROM <vertex_id_list> TO <vertex_id_list> OVER <edge_type_list> [UPTO <N> STEPS]
```

`SHORTEST` is the keyword to find the shortest path.

`ALL` is the keyword to find all path.

`<vertex_id_list>::=[vertex_id [, vertex_id]]` is the vertex id list, multiple ids should be separated with a comma, supporting input ```$-```and ```$var```.

`<edge_type_list>` is the edge type list, multiple edge types should be separated with a comma, ```*``` can be referred as all edge types.


`<N>` is hop number, the default value is 5.

## Note

- When start and end points are id lists, it means to find the shortest path from any start point to the end point.
- There may be cycles when searching all paths.

## Examples

Path is displayed as `id <edge_name, ranking> id` in console.

```
nebula> FIND SHORTEST PATH FROM 200 to 201 OVER *
============================
| _path_ |
============================
| 200 <like,0> 201
----------------------------
```

```
nebula> FIND ALL PATH FROM 200 to 201 OVER *
====================================================================================================
| _path_ |
====================================================================================================
| 200 <like,0> 201
----------------------------------------------------------------------------------------------------
| 200 <like,0> 201 <like,0> 200 <like,0> 201
----------------------------------------------------------------------------------------------------
| 200 <like,0> 201 <like,0> 200 <like,0> 201 <like,0> 200 <like,0> 201
----------------------------------------------------------------------------------------------------
```
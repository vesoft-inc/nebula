# Fetch Syntax

The `FETCH` syntax is used to get vertex/edge's properties.

## Fetch Vertex property

Use `FETCH PROP ON` to return a (list of) vertex's properties. Currently, you can get multiple vertices' properties with the same in one sentence.  

```sql
FETCH PROP ON <tag_name> <vertex_id_list> [YIELD [DISTINCT] <return_list>]
```

`<tag_name>` is the tag name. It should be the same tag within return_list

`<vertex_id_list>::=[vertex_id [, vertex_id]]` is a list of vertex id separated by comma(,)

`[YIELD [DISTINCT] <return_list>]` is the property list returned. Please refer here [YIELD Syntax](yield-syntax.md).

### Examples

```SQL
-- return all the properties of vertex id 1 if no yield field is given.
nebula> FETCH PROP ON player 1
-- return property name and age of vertex id 1
nebula> FETCH PROP ON player 1 YIELD player.name, player.age
-- hash string to int64 as vertex id, fetch name and player
nebula> FETCH PROP ON player hash(\"nebula\")  YIELD player.name, player.age
-- find all neighbors of vertex 1 through edge e1. Then Get the neighbors' name and age.
nebula> GO FROM 1 over e1 | FETCH PROP ON player $- YIELD player.name, player.age
-- the same as above sentence.
nebula> $var = GO FROM 1 over e1; FETCH PROP ON player $var.id YIELD player.name, player.age
-- get three vertices 1,2,3, return by unique(distinct) name and age
nebula> FETCH PROP ON player 1,2,3 YIELD DISTINCT player.name, player.age
```

## Fetch Edge Property

The `FETCH` usage of an edge is almost the same as for vertex.
You can get properties from multiple edges with the same type.

```sql
FETCH PROP ON <edge_type> <vid> -> <vid> [, <vid> -> <vid> ...] [YIELD [DISTINCT] <return_list>]
```

`<edge_type>` specifies the edge's type. It must be the same as those in `<return_list>`

`<vid> -> <vid>` denotes a starting vertex to (->) an ending vertex. Multiple edges are separated by comma(,).

`[YIELD [DISTINCT] <return_list>]` is the property list returned.

### Example

```SQL
-- from vertex 100 to 200 with edge type e1, get all the properties since no YIELD is given.
nebula> FETCH PROP ON e1 100 -> 200
-- only return property p1
nebula> FETCH PROP ON e1 100 -> 200 YIELD e1.p1
-- for all the out going edges of vertex 1, get edge property prop1.
nebula> GO FROM 1 OVER e1 YIELD e1.prop1
-- the same as above sentence
nebula> GO FROM 1 OVER e1 YIELD e1._src AS s, serve._dst AS d \
 | FETCH PROP ON e1 $-.s -> $-.d YIELD e1.prop1
-- the same as above.
nebula> $var = GO FROM 1 OVER e1 YIELD e1._src AS s, e2._dst AS d;\
 FETCH PROP ON e3 $var.s -> $var.d YIELD e3.prop1
```

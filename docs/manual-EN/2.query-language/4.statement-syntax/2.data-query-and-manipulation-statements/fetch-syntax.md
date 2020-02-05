# Fetch Syntax

The `FETCH` syntax is used to get vertex/edge's properties.

## Fetch Vertex property

Use `FETCH PROP ON` to return a (list of) vertex's properties. Currently, you can get multiple vertices' properties with the same tag in one statement.

```ngql
FETCH PROP ON <tag_name> <vertex_id_list> [YIELD [DISTINCT] <return_list>]
FETCH PROP ON * <vertex_id>
```

`*` indicates returning all the properties of the given vertex.

`<tag_name>` is the tag name. It must be the same tag within return_list.

`<vertex_id_list>::=[vertex_id [, vertex_id]]` is a list of vertex IDs separated by comma(,).

`[YIELD [DISTINCT] <return_list>]` is the property list returned. Please refer here [YIELD Syntax](yield-syntax.md).

### Examples

```ngql
-- return all the properties of vertex id 100.
nebula> FETCH PROP ON * 100;

-- return all the properties in tag player of vertex id 100 if no yield field is given.
nebula> FETCH PROP ON player 100;

-- return property name and age of vertex id 100.
nebula> FETCH PROP ON player 100 YIELD player.name, player.age;

-- hash string to int64 as vertex id, fetch name and player.
nebula> FETCH PROP ON player hash("nebula")  YIELD player.name, player.age;

-- find all neighbors of vertex 100 through edge follow. Then get the neighbors' name and age.
nebula> GO FROM 100 OVER follow YIELD follow._dst AS id | FETCH PROP ON player $-.id YIELD player.name, player.age;

-- the same as above statement.
nebula> $var = GO FROM 100 OVER follow YIELD follow._dst AS id; FETCH PROP ON player $var.id YIELD player.name, player.age;

-- get three vertices 100, 101, 102 and return by unique(distinct) name and age.
nebula> FETCH PROP ON player 100,101,102 YIELD DISTINCT player.name, player.age;
```

## Fetch Edge Property

The `FETCH` usage of an edge is almost the same with vertex.
You can get properties from multiple edges with the same type.

```ngql
FETCH PROP ON <edge_type> <vid> -> <vid>[@<ranking>] [, <vid> -> <vid> ...] [YIELD [DISTINCT] <return_list>]
```

`<edge_type>` specifies the edge's type. It must be the same as those in `<return_list>`.

`<vid> -> <vid>` denotes a starting vertex to (->) an ending vertex. Multiple edges are separated by comma(,).

`<ranking>` specifies the edge weight of the same edge type; it's optional.

`[YIELD [DISTINCT] <return_list>]` is the property list returned.

### Example

```ngql
-- from vertex 100 to 200 with edge type serve, get all the properties since no YIELD is given.
nebula> FETCH PROP ON serve 100 -> 200;

-- only return property start_year.
nebula> FETCH PROP ON serve 100 -> 200 YIELD serve.start_year;

-- for all the out going edges of vertex 100, get edge property degree.
nebula> GO FROM 100 OVER follow YIELD follow.degree;

-- the same as above statement.
nebula> GO FROM 100 OVER follow YIELD follow._src AS s, follow._dst AS d \
 | FETCH PROP ON follow $-.s -> $-.d YIELD follow.degree;

-- the same as above.
nebula> $var = GO FROM 100 OVER follow YIELD follow._src AS s, follow._dst AS d;\
 FETCH PROP ON follow $var.s -> $var.d YIELD follow.degree;
```



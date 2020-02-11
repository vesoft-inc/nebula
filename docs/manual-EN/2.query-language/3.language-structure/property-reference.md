# Property Reference

You can refer a vertex or edge's property in `WHERE` or `YIELD` syntax.

## Reference From Vertex

### For Source Vertex

```ngql
$^.tag_name.prop_name
```

where symbol `$^` is used to get a source vertex's property,
`tag_name` indicates the source vertex's `tag`,
and `prop_name` specifies the property name.

### For Destination Vertex

```ngql
$$.tag_name.prop_name
```

Symbol `$$` indicates the ending vertex, `tag_name` and `prop_name` are the vertex's tag and property respectively.

### Example

```ngql
nebula> GO FROM 100 OVER follow YIELD $^.player.name AS StartName, $$.player.age AS EndAge;
```

Use the above query to get the source vertex's property name and ending vertex's property age.

## Reference From Edge

### For Property

You can use the following syntax to get an edge's property.

```ngql
edge_type.edge_prop
```

`edge_type` is the edge's type, meanwhile `edge_prop` is the property.

For example,

```ngql
nebula> GO FROM 100 OVER follow YIELD follow.degree;
```

### For Built-in Properties

There are four built-in properties in the edge:

* _src: source vertex ID of the edge
* _dst: destination ID of the edge
* _type: edge type
* _rank: the edge's ranking

You can use `_src` and `_dst` to get the starting and ending vertices' ID, and they are very commonly used to show a graph path.

For example,

```ngql
nebula> GO FROM 100 OVER follow YIELD follow._src AS startVID /* starting vertex is 100 */, follow._dst AS endVID;
```

This statement returns all the neighbors of vertex `100` over edge type `follow`, by referencing `follow._src` as the starting vertex ID (which, of course, is `100`) and `follow._dst` as the ending vertex ID.

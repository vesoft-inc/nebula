# Property Reference

You can refer a vertex or edge's property in `WHERE` or `YIELD` syntax.

## Reference from vertex

### For source vertex

```
$^.tag_name.prop_name
```

where symbol `$^` is used to get a source vertex's property,
`tag_name` indicates the source vertex's `tag`,
and `prop_name` specifies the property name.

### For destination vertex

```
$$.tag_name.prop_name
```

where symbol `$$` indicates the ending vertex, `tag_name` and `prop_name` are the vertex's tag and property respectively.

### Example

```
GO FROM 1 OVER e1 YIELD $^.start.name AS startName, $$.end.Age AS endAge
```

to get the starting vertex's property name and ending vertex's property age.

## Reference from edge

### For property

You can use the following to get an edge's property.

```
edge_type.edge_prop
```

where `edge_type` is the edge's type, meanwhile `edge_prop` is the property.

For example,

```
GO FROM 1 OVER e1 YIELD e1.prop1
```

### For build-in properties

There are four build-in properties in the edge:

* _src: source vertex id of the edge
* _dst: destination id of the edge
* _type: edge type
* _rank: the edge's ranking

You can use `_src` and `_dst` to get the starting and ending vertices' id, and they are very commonly used to show a graph path.

For example,

```
GO FROM 1 OVER e1 YIELD e1._src as startVID /* which is, 1 */, e1._dst as endVID
```

This statement returns all the neighbors of vertex `1` over edge type `e1`, by referencing `e1._src` as the starting vertex id (which, of course, is `1`) and `e1._dst` as the ending vertex id.

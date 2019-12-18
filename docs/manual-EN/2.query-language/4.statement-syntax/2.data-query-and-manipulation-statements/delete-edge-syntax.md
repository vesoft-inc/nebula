# Delete Edge Syntax

The `DELETE EDGE` statement is used to delete edges. Given an edge type, the source vertex and the dest vertex, **Nebula Graph** supports `DELETE` the edge, its associated properties and the edge ranking. You can also delete an edge with a certain rank. The syntax is as follows:

```ngql
DELETE EDGE <edge_type> <vid> -> <vid>[@<ranking>] [, <vid> -> <vid> ...]
```

**Nebula Graph** will find the properties associated with the edge and delete all of them. Atomic operation is not guaranteed during the entire process for now, so please retry when failure occurs.

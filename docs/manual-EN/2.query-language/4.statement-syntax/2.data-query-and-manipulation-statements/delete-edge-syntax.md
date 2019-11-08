# Delete Edge Syntax

Given an edge type, the source vertex and the dest vertex, Nebula supports `DELETE` the edge and its associated properties, syntax as the follows:

```ngql
DELETE EDGE <edge_type> <vid> -> <vid> [, <vid> -> <vid> ...]
```

Nebula will find the properties associated with the edge and delete all of them. Atomic operation is not guaranteed during the entire process for now, so please retry when failure occurs.

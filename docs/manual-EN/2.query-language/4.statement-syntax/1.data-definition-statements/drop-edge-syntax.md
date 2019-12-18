# Drop Edge Syntax

```ngql
DROP EDGE <edge_type_name>
```

You must have the DROP privilege for the edge type.

This statement removes all the edges (connections) within the specific edge type.

This operation only deletes the Schema data, all the files and directories in the disk are NOT deleted directly, data is deleted in the next compaction.

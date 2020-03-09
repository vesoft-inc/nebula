# Drop Edge Syntax

```ngql
DROP EDGE [IF EXISTS] <edge_type_name>
```

You must have the DROP privilege for the edge type.

**Note:** When dropping an edge, **Nebula Graph** only checks whether the edge is associated with any indexes. If so the deletion is rejected.

Please refer to [Index Documentation](index.md) on details about index.

You can use the `If EXISTS` keywords when dropping edges. These keywords automatically detect if the corresponding edge exists. If it exists, it will be deleted. Otherwise, no edge is deleted.

This statement removes all the edges (connections) within the specific edge type.

This operation only deletes the Schema data, all the files and directories in the disk are NOT deleted directly, data is deleted in the next compaction.

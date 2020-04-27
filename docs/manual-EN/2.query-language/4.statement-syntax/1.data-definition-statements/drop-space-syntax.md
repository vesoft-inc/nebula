# Drop Space Syntax

```ngql
DROP SPACE [IF EXISTS] <space_name>
```

You must have the DROP privilege for the graph space.

DROP SPACE deletes everything (all the vertices, edges, indices, and properties) in the specific space.  

You can use the `If EXISTS` keywords when dropping spaces. This keyword automatically detects if the corresponding space exists. If it exists, it will be deleted. Otherwise, no space is deleted.

Other spaces remain unchanged.

This statement does not immediately remove all the files and directories in the storage engine (and release disk space). The deletion depends on the implementation of different storage engines.

> Be *very* careful with this statement.

```
DROP SPACE space_name
```
You must have the DROP privilege for the graph space.
DROP SPACE deletes everyting (all the vertices, edges, indices, and properties) in the specific space.  
Other spaces remain unchanged.
This statement does not immediately remove all the files and directories that holds in the storage engine (and release disk space). The deletetion depends on the implement of  different storage engine.

>Be *very* careful with this statement.


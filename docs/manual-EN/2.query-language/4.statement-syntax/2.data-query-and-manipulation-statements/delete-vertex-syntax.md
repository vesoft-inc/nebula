# Delete Syntax

Given a vertex Id, **Nebula Graph** supports `DELETE` the vertex and its associated in and out edges, syntax as the follows:

```ngql
DELETE VERTEX <vid>
```

**Nebula Graph** will find the in and out edges associated with the vertex and delete all of them, then delete information related to the vertex. Atomic operation is not guaranteed during the entire process for now, so please retry when failure occurs.

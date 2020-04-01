# Delete Syntax

Given a list of vertices IDs, hash IDs or UUIDs, **Nebula Graph** supports `DELETE` the vertices and their associated in and out edges, syntax as the follows:

```ngql
DELETE VERTEX <vid_list>
```

**Nebula Graph** will find the in and out edges associated with the vertices and delete all of them, then delete information related to the vertices. Atomic operation is not guaranteed during the entire process for now, so please retry when failure occurs.

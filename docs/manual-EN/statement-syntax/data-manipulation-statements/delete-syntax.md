# Delete Syntax
Given a vetex Id, Nebula supports `DELETE` the vertex and its associated in and out edges, syntax as the follows:

```
DELETE VERTEX $vid
```
Nebula will find the in and out edges associated with the vertex and delete all of them, then delete information related to the vertex. ATOM is not guaranteed during the entire process for now.
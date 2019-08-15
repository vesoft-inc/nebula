
One major difference between nGQL and SQL is how subqueries are composed.

In SQL, sub-queries are nested (embedded) to form a statement.
MeanWhile, nGQL uses shell style `PIPE (|)`.

### Examples

```
GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...

GO FROM 100 OVER like YIELD like._dst AS Id, $$.player.name AS Name  | GO FROM $-.Id OVER like YIELD like._dst, like.likeness, $-.Name
```
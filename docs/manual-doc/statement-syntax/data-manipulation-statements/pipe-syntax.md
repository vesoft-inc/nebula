# PIPE-syntax

One major difference between nGQL and SQL is how subqueries are composed.

In SQL, sub-queries are nested (embedded) to form a statement.
MeanWhile, nGQL uses shell style `PIPE (|)`.

### Examples

```
GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...

GO FROM 100 OVER like YIELD like._dst AS dstid, $$.player.name AS Name  | GO FROM $-.dstid OVER like YIELD like._dst, like.likeness, $-.Name
```

The alias name right after placeholder `$-.` must be either exactly `id` or defined in the previews statement (e.g., `distid` or `Name` as show in the above example).

(As a syntax sugar, you can use `$-` in stead of `$-.id` for short)


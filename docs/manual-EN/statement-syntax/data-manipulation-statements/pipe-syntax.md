# PIPE-syntax

One major difference between nGQL and SQL is how subqueries are composed.

In SQL, sub-queries are nested (embedded) to form a statement.
Meanwhile, nGQL uses shell style `PIPE (|)`.

### Examples

```
GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...

GO FROM 100 OVER like YIELD like._dst AS dstid, $$.player.name AS Name  | GO FROM $-.dstid OVER like YIELD like._dst, like.likeness, $-.Name
```

The dest (vertex) `id` will be given as the default value if no `YIELD` is used. 

But if `YIELD` is declared explicitly, (the default value) `id` will not be given.

The alias name mentioned right after placeholder `$-.` must be either exactly `id` or already defined in the previews `YIELD` statement (e.g., `distid` or `Name` as shown in the above example).

(As a syntax sugar, you can use `$-` to `$-.id` for short)

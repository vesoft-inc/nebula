# PIPE Syntax

One major difference between nGQL and SQL is how sub-queries are composed.

In SQL, sub-queries are nested (embedded) to form a statement.
Meanwhile, nGQL uses shell style `PIPE (|)`.

## Examples

```ngql
GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...

GO FROM 100 OVER follow YIELD follow._dst AS dstid, $$.player.name AS Name  | GO FROM $-.dstid OVER follow YIELD follow._dst, follow.likeness, $-.Name
```

The dest (vertex) `id` will be given as the default value if no `YIELD` is used.

But if `YIELD` is declared explicitly, (the default value) `id` will not be given.

The alias name mentioned right after placeholder `$-.` must be either exactly `id` or already defined in the previews `YIELD` statement (e.g., `dstid` or `Name` as shown in the above example).

(As a syntactic sugar, you can use `$-` to `$-.id` for short)

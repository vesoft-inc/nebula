# Truncate Space Syntax

```ngql
TRUNCATE SPACE <space_name>
```

`TRUNCATE SPACE` empties a graph space completely but reserves the schema (including the indexes and the default values). It requires the DROP privilege. Logically, `TRUNCATE SPACE` is similar to a series of `DELETE VERTEX` and `DELETE EDGE` statements that deletes all vertices and edges, or a sequence of `DROP SPACE` and `CREATE SPACE` statements.

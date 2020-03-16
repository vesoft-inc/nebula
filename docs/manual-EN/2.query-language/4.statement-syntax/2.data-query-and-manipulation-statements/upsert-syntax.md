# Upsert Syntax

`UPSERT` is used to insert a new vertex or edge or update an existing one. If the vertex or edge doesn’t exist it will be created. `UPSERT` is a combination of `INSERT` and `UPDATE`.

- If the vertex or edge does not exist, a new one will be created regardless of whether the condition in WHEN clause is met;
- If the vertex or edge exists and the WHEN condition is met, the vertex or edge will be updated;
- If the vertex or edge exists and the WHEN condition is not met, nothing will be done.

```ngql
UPSERT {VERTEX <vid> | EDGE <edge>} SET <update_columns> [WHEN <condition>] [YIELD <columns>]
```

- `vid` is the ID of the vertex to be updated.
- `edge` is the edge to be updated, the syntax is `$src->$dst[@ranking] OF $type`.
- `update_columns` is the properties of the vertex or edge to be updated, for example, `tag1.col1 = $^.tag2.col2 + 1` means to update `tag1.col1` to `tag2.col2+1`.

    **NOTE:**  `$^` indicates vertex to be updated.

- `condition` is some constraints, only when met, `UPSERT` will run successfully and expression operations are supported.
- `columns` is the columns to be returned, `YIELD` returns the latest updated values.

Consider the following example:

```ngql
nebula> INSERT VERTEX player(name, age) VALUES 111:("Ben Simmons", 22); -- Insert a new vertex.
nebula> UPSERT VERTEX 111 SET player.name = "Dwight Howard", player.age = $^.player.age + 11 WHEN $^.player.name == "Ben Simmons" && $^.player.age > 20 YIELD $^.player.name AS Name, $^.player.age AS Age; -- Do upsert on the vertex.
=======================
| Name          | Age |
=======================
| Dwight Howard | 33  |
-----------------------
```

# UPSERT 语法

`UPSERT` 用于插入新的顶点或边或更新现有的顶点或边。如果顶点或边不存在，则会新建该顶点或边。`UPSERT` 是 `INSERT` 和 `UPDATE` 的组合。

- 如果顶点或边不存在，则会新建该顶点或边，无论 WHEN 条件是否满足；
- 如果该顶点或者边存在，并且 WHEN 条件满足，则会更新；
- 如果该顶点或者边存在, 并且 WHEN 条件不满足，则不会有任何操作。

```ngql
UPSERT {VERTEX <vid> | EDGE <edge>} SET <update_columns> [WHEN <condition>] [YIELD <columns>]
```

- `vid` 表示需要更新的 vertex ID。
- `edge` 表示需要更新的 edge，edge 的格式为 `<src> -> <dst> [@ranking] OF <edge_type>`。
- `update_columns` 表示需要更新的 tag 或 edge 上的 columns，比如 `tag1.col1 = $^.tag2.col2 + 1` 表示把这个点的 `tag1.col1` 更新成 `tag2.col2 + 1`。

    **注意：**  `$^`表示 `UPDATE` 中需要更新的点。

- `condition` 是一些约束条件，只有满足这个条件，`UPDATE` 才会真正执行，支持表达式操作。
- `columns` 表示需要返回的 columns，此处 `YIELD` 可返回 update 以后最新的 columns 值。

例如：

```ngql
nebula> INSERT VERTEX player(name, age) VALUES 111:("Ben Simmons", 22); -- 插入一个新点。
nebula> UPSERT VERTEX 111 SET player.name = "Dwight Howard", player.age = $^.player.age + 11 WHEN $^.player.name == "Ben Simmons" && $^.player.age > 20 YIELD $^.player.name AS Name, $^.player.age AS Age; -- 对该点进行 UPSERT 操作。
=======================
| Name          | Age |
=======================
| Dwight Howard | 33  |
-----------------------
```

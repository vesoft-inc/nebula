# UUID

`UUID` 用于生成全局唯一的标识符。

当顶点数量到达十亿级别时，用 `hash` 函数生成 vid 有一定的冲突概率。因此 `Nebula` 提供 `UUID` 函数来避免大量顶点时的 vid 冲突。 `UUID` 函数由 `Murmurhash` 与当前时间戳（单位为秒）组合而成。

`UUID` 产生的值会以 key-value 方式存储在 `Nebula` 的 Storage 服务中，调用时会检查这个 key 是否存在或冲突。因此相比 hash，性能可能会更慢。

## 示例

Insert with UUID:

```SQL
-- INSERT VERTEX player (name, age) VALUES hash("n0"):("n0", 13.0)
nebula> INSERT VERTEX player (name, age) VALUES uuid("n0"):("n0", 13.0)
-- INSERT EDGE like(likeness) VALUES hash("n0") -> hash("n1"): (90.0)
nebula> INSERT EDGE like(likeness) VALUES uuid("n0") -> uuid("n1"): (90.0)
```

Fetch with UUID:

```SQL
nebula> FETCH PROP ON player uuid("n0")  YIELD player.name, player.age
nebula> FETCH PROP ON like uuid("n0") -> uuid("n1")
```

Go with UUID:

```SQL
nebula> GO FROM uuid("n0") OVER like
```

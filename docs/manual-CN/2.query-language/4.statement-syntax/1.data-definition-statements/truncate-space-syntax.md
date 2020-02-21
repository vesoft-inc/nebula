# TRUNCATE SPACE 语法

```ngql
TRUNCATE SPACE <space_name>
```

`TRUNCATE SPACE` 可清空图空间内所有的点边数据，但是会保留 schema（包括索引和默认值）。此操作需要 DROP 权限。逻辑上，`TRUNCATE SPACE` 类似于一系列 `DELETE VERTEX` 和 `DELETE EDGE` 语句，该语句删除所有顶点和边，或一系列 `DROP SPACE` 和 `CREATE SPACE` 语句。

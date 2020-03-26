# SHOW CREATE TAGS/EDGES 语法

```ngql
SHOW CREATE {TAG <tag_name> | EDGE <edge_name>}
```

`SHOW CREATE TAG` 和 `SHOW CREATE EDGE` 返回当前图空间中指定的 tag、edge type 及其创建语法。如果 tag 或 edge type 包含默认值，则同时返回默认值。

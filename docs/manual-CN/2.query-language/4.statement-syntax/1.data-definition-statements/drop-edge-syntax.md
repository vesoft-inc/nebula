# DROP EDGE 语法

```ngql
DROP EDGE <edge_type_name>
```

仅支持有 DROP 权限的用户进行此操作。

此操作将移除指定类型的所有边。

此操作仅删除 Schema 信息，硬盘中所有文件及目录均**未被直接删除**，数据会在下次 compaction 时删除。

# DROP SPACE 语法

```ngql
DROP SPACE <space_name<>
```

仅支持有 DROP 权限的用户进行此操作。
DROP SPACE 将删除指定 space 内的所有点和边。

其他 space 不受影响。

该语句不会立即删除存储引擎中的所有文件和目录（并释放磁盘空间）。删除操作取决于不同存储引擎的实现。

> 请**谨慎**进行此操作。

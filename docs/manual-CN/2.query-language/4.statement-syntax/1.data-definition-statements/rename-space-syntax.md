# RENAME SPACE 语法

```ngql
RENAME SPACE <space_name> TO <new_space_name>
```

`RENAME SPACE` 重命名一个图空间。此操作需要具有原始空间的 `DROP` 权限，以及新空间的 `CREATE` 和 `INSERT` 权限。

例如，使用以下语句将 _old_space_ 重命名为 _new_space_：

```ngql
nebula> RENAME SPACE old_graph TO new_graph
```

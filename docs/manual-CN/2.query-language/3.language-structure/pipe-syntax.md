# 管道

nGQL和SQL的主要区别之一是子查询的组合方式。

SQL中的查询语法通常由子查询嵌套组成，而nGQL则使用类似于shell的管道方式`PIPE(|)`来组合子查询

## 示例

```SQL
nebula> GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...
nebula> GO FROM 100 OVER like YIELD like._dst AS Id, $$.player.name AS Name \
  | GO FROM $-.Id OVER like YIELD like._dst, like.likeness, $-.Name
```

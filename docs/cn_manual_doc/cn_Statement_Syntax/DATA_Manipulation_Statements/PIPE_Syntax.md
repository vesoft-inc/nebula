
nGQL和SQL的主要区别之一是子句的组合方式。
SQL中的子句嵌套在一条语句中，而nGQL则使用类型于shell的`PIPE (|)`。

### 示例

```
GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...

GO FROM 100 OVER like YIELD like._dst AS Id, $$.player.name AS Name  | GO FROM $-.Id OVER like YIELD like._dst, like.likeness, $-.Name
```

# 管道

nGQL 和 SQL 的主要区别之一是子查询的组合方式。

SQL 中的查询语句通常由子查询嵌套组成，而 nGQL 则使用类似于 shell 的管道方式 `PIPE(|)` 来组合子查询。

## 示例

```ngql
GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...

GO FROM 100 OVER follow YIELD follow._dst AS dstid, $$.player.name AS Name  | GO FROM $-.dstid OVER follow YIELD follow._dst, follow.likeness, $-.Name
```

如未使用 `YIELD`，则默认返回终点 `id`。

如果使用 `YIELD` 明确声明返回结果，则不会返回默认值 `id`。

`$-.` 后的别名必须为准确的点 `id` 或前一个子句 `YIELD` 定义的值，如本例中的 `dstid` 和 `Name`。
（作为语法糖，`$-.id` 可简化为 `$-`。）

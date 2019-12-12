# WHERE 语法

目前，`WHERE` 语句仅适用于 `GO` 语句。

```ngql
WHERE <expression> [ AND | OR <expression> ...])  
```

通常，筛选条件是关于节点、边的表达式的逻辑组合。

> 作为语法糖，逻辑与可用 `AND` 或 `&&`，同理，逻辑或可用 `OR` 或 `||` 表示。

## 示例

```ngql
-- 边 e1 的 prop1 属性大于 17
nebula> GO FROM 201 OVER e1 WHERE e1.prop1 >= 17
-- 起点 v1 的 prop1 属性与终点 v2 的 prop2 属性值相等
nebula> GO FROM 201 OVER e1 WHERE $^.v1.prop1 == $$.v2.prop2
-- 多种逻辑组合
nebula> GO FROM 201 OVER e1 WHERE ((e3.prop3 < 0.5) \
   OR ($^.v4.prop4 != "hello")) AND $$.v5.prop5 == "world"
--下面这个条件总是为 TRUE
nebula> GO FROM 201 OVER e1 WHERE 1 == 1 OR TRUE
```

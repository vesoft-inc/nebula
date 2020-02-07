# WHERE 语法

目前，`WHERE` 语句仅适用于 `GO` 语句。

```ngql
WHERE <expression> [ AND | OR <expression> ...])  
```

通常，筛选条件是关于节点、边的表达式的逻辑组合。

> 作为语法糖，逻辑与可用 `AND` 或 `&&`，同理，逻辑或可用 `OR` 或 `||` 表示。

## 示例

```ngql
-- 边 follow 的 degree 属性大于 90。
nebula> GO FROM 100 OVER follow WHERE follow.degree>90;
-- 返回以下值：
===============
| follow._dst |
===============
| 101         |
---------------
-- 起点 player 104 的 age 属性与终点 player 103 的 age 属性值相等。
nebula> GO FROM 104 OVER follow WHERE $^.player.age == $$.player.age;
-- 返回以下值：
===============
| follow._dst |
===============
| 103         |
---------------
-- 多种逻辑组合。
nebula> GO FROM 100 OVER follow WHERE follow.degree > 90 OR $$.player.age != 33 AND $$.player.name != "Tony Parker";
-- 返回以下值：
===============
| follow._dst |
===============
| 101         |
---------------
| 106         |
---------------
--下面这个条件总是为 TRUE。
nebula> GO FROM 101 OVER follow WHERE 1 == 1 OR TRUE;
-- 返回以下值：
===============
| follow._dst |
===============
| 100         |
---------------
| 102         |
---------------
```

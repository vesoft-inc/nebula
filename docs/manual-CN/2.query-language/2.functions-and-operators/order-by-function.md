# Order By 函数

类似于 SQL，`ORDER BY` 可以进行升序 (`ASC`) 或降序 (`DESC`) 排序并返回结果，并且它只能在 `PIPE` 语句 (`|`) 中使用。

```ngql
ORDER BY <expression> [ASC | DESC] [, <expression> [ASC | DESC] ...]
```

如果没有指明 ASC 或 DESC，`ORDER BY` 将默认进行升序排序。

## 示例

```ngql
nebula> FETCH PROP ON player 100,101,102,103 YIELD player.age AS age, player.name AS name | ORDER BY age, name DESC;  

-- 取 4 个顶点并将他们以 age 从小到大的顺序排列，如 age 相同，则 name 按降序排列。
-- 返回如下结果:
======================================
| VertexID | age | name              |
======================================
| 103      | 32  | Rudy Gay          |
--------------------------------------
| 102      | 33  | LaMarcus Aldridge |
--------------------------------------
| 101      | 36  | Tony Parker       |
--------------------------------------
| 100      | 42  | Tim Duncan        |
--------------------------------------
```

(使用方法参见 [FETCH](../4.statement-syntax/2.data-query-and-manipulation-statements/fetch-syntax.md) 文档)

```ngql
nebula> GO FROM 100 OVER follow YIELD $$.player.age AS age, $$.player.name AS name | ORDER BY age DESC, name ASC;

-- 从顶点 100 出发查找其关注的球员，返回球员的 age 和 name，age 按降序排序，如 age 相同，则 name 按升序排序。
-- 返回如下结果：
===========================
| age | name              |
===========================
| 36  | Tony Parker       |
---------------------------
| 33  | LaMarcus Aldridge |
---------------------------
| 25  | Kyle Anderson     |
---------------------------
```

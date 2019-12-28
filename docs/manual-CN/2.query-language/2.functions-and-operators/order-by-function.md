# Order By 函数

类似于 SQL，`ORDER BY` 可以进行升序 (`ASC`) 或降序 (`DESC`) 的排序来返回结果，并且它只能在 `PIPE` 语句 (`|`) 中使用。

```ngql
ORDER BY <expression> [ASC | DESC] [, <expression> [ASC | DESC] ...]
```

如果没有指明 ASC 或 DESC，`ORDER BY` 将默认进行升序排序。

## 示例

```ngql
nebula> FETCH PROP ON player 1,2,3,4 YIELD player.age AS age, player.weight AS weight | ORDER BY $-.age, $-.weight DESC  

-- 取 4 个顶点并将他们以 age 从小到大的顺序排列，如 age 一致，则按 weight 从大到小的顺序排列。
```

(使用方法参见 [FETCH](../4.statement-syntax/2.data-query-and-manipulation-statements/fetch-syntax.md) 文档)

```ngql
nebula> GO FROM 1 OVER edge2 YIELD $^.t1.prop1 AS s1_p1, edge2.prop2 AS e2_p2, $$.t3.prop3 AS d3_p3 | ORDER BY s1_p1 ASC, e2_p2 DESC, d3_p3 ASC

-- 返回类似如下的列表
   ==========================
   | s1_p1  | e2_p2 | d3_p3 |
   --------------------------
   |   123  |  345  |  234  |
   |   234  |  32   |   0   |
   |   234  |  31   |   0   |
   |   234  |  31   |   1   |
   ==========================
   第一列按升序排列，第二列按降序排列，第三列按升序排列
```

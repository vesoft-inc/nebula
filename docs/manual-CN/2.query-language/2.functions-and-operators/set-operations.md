# 集合操作 (`UNION`，`INTERSECT`， `MINUS`)

## UNION，UNION DISTINCT，UNION ALL

`UNION DISTINCT` (简称 `UNION`)返回数据集 A 和 B 的并集（不包含重复元素）。

`UNION ALL` 返回数据集 A 和 B 的并集（包含重复元素）。`UNION` 语法为

```ngql
<left> UNION [DISTINCT | ALL] <right> [ UNION [DISTINCT | ALL] <right> ...]
```

`<left>` 和 `<right>` 必须列数相同，且数据类型相同。如果数据类型不同，将按照[类型转换](../1.data-types/type-conversion.md)进行转换。

### 示例

```ngql
nebula> GO FROM 1 OVER e1 \
        UNION \
        GO FROM 2 OVER e1
```

以上语句返回点 `1` 和 `2` (沿边 `e1`) 关联的唯一的点。

```ngql
nebula> GO FROM 1 OVER e1 \
        UNION ALL\
        GO FROM 2 OVER e1
```

以上语句返回点 `1` 和 `2` 关联的所有点，其中存在重复点。

`UNION` 亦可与 `YIELD` 同时使用，例如以下语句：

```ngql
nebula> GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left_1, $$.tag.prop2 AS left_2 -- query 1
==========================
| id  | left_1 | left_2  |
==========================
| 104 |    1   |    2    |    -- line 1
--------------------------
| 215 |    4   |    3    |    -- line 3
--------------------------

nebula> GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right_1, $$.tag.prop2 AS right_2  -- query 2
===========================
| id  | right_1 | right_2 |
===========================
| 104 |    1    |    2    |    -- line 1
---------------------------
| 104 |    2    |    2    |    -- line 2
---------------------------
```

```ngql
nebula> GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left_1, $$.tag.prop2 AS left_2   \
        UNION /* DISTINCT */     \
        GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right_1, $$.tag.prop2 AS right_2
```

以上语句返回

```ngql
=========================
| id  | left_1 | left_2 |    -- UNION or UNION DISTINCT. The column names come from query 1
=========================
| 104 |    1   |    2   |    -- line 1
-------------------------
| 104 |    2   |    2   |    -- line 2
-------------------------
| 215 |    4   |    3   |    -- line 3
-------------------------
```

请注意第一行与第二行返回相同 id 的点，但是返回的值不同。`DISTINCT` 检查返回结果中的重复值。所以第一行与第二行的返回结果不同。
`UNION ALL` 返回结果为

```ngql
nebula> GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left_1, $$.tag.prop2 AS left_2   \
        UNION ALL   \
        GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right_1, $$.tag.prop2 AS right_2

=========================
| id  | left_1 | left_2 |    -- UNION ALL
=========================
| 104 |    1   |    2   |    -- line 1
-------------------------
| 104 |    1   |    2   |    -- line 1
-------------------------
| 104 |    2   |    2   |    -- line 2
-------------------------
| 215 |    4   |    3   |    -- line 3
-------------------------
```

## INTERSECT

`INTERSECT` 返回集合 A 和 B ( A ⋂ B)的交集。

```ngql
<left> INTERSECT <right>
```

与 `UNION` 类似， `<left>` 和 `<right>` 必须列数相同，且数据类型相同。
此外，只返回 `<left>` 右 `<right>` 相同的行。例如：

```ngql
nebula> GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left_1, $$.tag.prop2 AS left_2
INTERSECT
GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right_1, $$.tag.prop2 AS right_2
```

返回

```ngql
=========================
| id  | left_1 | left_2 |
=========================
| 104 |    1   |    2   |    -- line 1
-------------------------
```

## MINUS

返回 A - B 数据的差集，此处请注意运算顺序。例如：

```ngql
nebula> GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left_1, $$.tag.prop2 AS left_2
MINUS
GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right_1, $$.tag.prop2 AS right_2
```

返回

```ngql
==========================
| id  | left_1 | left_2  |
==========================
| 215 |    4   |    3    |     -- line 3
--------------------------
```

如果更改 `MINUS` 顺序

```ngql
nebula> GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right_1, $$.tag.prop2 AS right_2
MINUS
GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left_1, $$.tag.prop2 AS left_2
```

则返回

```ngql
===========================
| id  | right_1 | right_2 |    -- column named from query 2
===========================
| 104 |    2    |    2    |    -- line 2
---------------------------
```

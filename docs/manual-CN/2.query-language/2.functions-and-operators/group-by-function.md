# 聚合函数 (Group By)

 `GROUP BY` 函数类似于 SQL。 只能与 `YIELD` 语句一起使用。

|名称 | 描述 |
|:----|:----:|
| AVG()           | 返回参数的平均值 |
| COUNT()         | 返回记录值总数 |
| COUNT_DISTINCT()) | 返回独立记录值的总数 |
| MAX()           | 返回最大值 |
| MIN()           | 返回最小值 |
| STD()           | 返回总体标准差 |
| SUM()         | 返回总合 |
| BIT_AND()      |   按位与 |
| BIT_OR()        |   按位或 |
| BIT_XOR()     |   按位异或 |
以上函数只作用于 int64 和 double。

## 示例

```ngql
nebula> GO FROM 100 OVER follow YIELD $$.player.name as Name | GROUP BY $-.Name YIELD $-.Name, COUNT(*);
-- 从节点 100 出发，查找其关注的球员并返回球员的姓名作为 Name，按照姓名对球员分组并统计每个分组的人数。
-- 返回以下结果：
================================
| $-.Name           | COUNT(*) |
================================
| Kyle Anderson     | 1        |
--------------------------------
| Tony Parker       | 1        |
--------------------------------
| LaMarcus Aldridge | 1        |
--------------------------------

nebula> GO FROM 101 OVER follow YIELD follow._src AS player, follow.degree AS degree | GROUP BY $-.player YIELD SUM($-.degree);
-- 从节点 101 出发找到其关注的球员，返回这些球员作为 player，边（follow）的属性值作为 degree，对这些球员分组并返回分组球员属性 degree 相加的值。
-- 返回以下结果：
==================
| SUM($-.degree) |
==================
| 186            |
------------------
```

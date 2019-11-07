# 聚合函数 (Group by)

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
nebula> GO FROM 1 OVER e1 YIELD $-.id AS fid | GROUP BY $-.fid YIELD COUNT(*)
-- 统计与节点 "1" 有 e1 关系的点的的数量

nebula> GO FROM 1 YIELD e1._dst AS fid, e1.prop1 AS prop1 | GROUP BY $-.fid YIELD SUM($-.prop1)
-- 对与节点 "1" 有 e1 关系的点的属性 prop1 进行加法运算
```

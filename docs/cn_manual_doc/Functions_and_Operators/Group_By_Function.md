
# 合计函数 (Group by)

 `GROUP BY` 函数类似于SQL。 它们只能在 `YIELD` 语法下应用.

|名称 | 描述 |
|:----|:----:|
| AVG()           | 返回参数的平均值 |
| COUNT()         | 返回记录值总数 |
| COUNT(DISTINCT) | 返回独立记录值的总数 |
| MAX()           | 返回最大值 |
| MIN()           | 返回最小值 |
| STD()           | 返回总体标准差 | 
| SUM()	          | 返回总合 |

以上函数只作用于 int64 和 double。

### 示例

```
nebula> GO FROM 1 OVER e1 | YIELD $-.id AS fid, COUNT(*) AS cnt GROUP BY fid
-- 对于每一个 fid, return the occurrence count.

nebula> GO FROM 1 YIELD e1._dst AS fid, e1.prop1 AS prop1 | YIELD fid, SUM(prop1) GROUP BY fid
-- 对于每一个 fid, 返回prop1的总合。
```

# Aggregate (Group by) function

The `GROUP BY` functions  are similar with SQL. It can only be applied in the `YIELD`-syntax.

|Name | Description |
|:----|:----:|
| AVG() | Return the average value of the argument |
| COUNT() | Return the number of records |
| COUNT(DISTINCT) | Return the number of different values |
| MAX() | Return the maximum value |
| MIN() | Return the minimum value |
| STD() | Return the population standard deviation | 
| SUM()	| Return the sum |

All the functions above can only applies for int64 and double.

### Example

```
nebula> GO FROM 1 OVER e1 | YIELD $-.id AS fid, COUNT(*) AS cnt GROUP BY fid
-- for each fid, return the occurrence count.

nebula> GO FROM 1 YIELD e1._dst AS fid, e1.prop1 AS prop1 | YIELD fid, SUM(prop1) GROUP BY fid
-- for each fid, return the sum of prop1.
```
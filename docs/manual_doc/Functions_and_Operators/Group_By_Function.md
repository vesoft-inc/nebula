
### Aggregate (Group by) function

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

### example

```
nebula> GO FROM 1 OVER e1 | YIELD $-.id AS fid, COUNT(*) AS cnt GROUP BY fid
nebula> GO FROM 1 YIELD e1._dst AS fid, e1.prop1 AS prop1 | YIELD fid, SUM(prop1) GROUP BY fid
```
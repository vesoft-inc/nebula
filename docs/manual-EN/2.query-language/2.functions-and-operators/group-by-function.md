# Aggregate (Group by) Function

The `GROUP BY` functions are similar with SQL. It can only be applied in the `YIELD` syntax.

|Name | Description |
|:----|:----:|
| AVG() | Return the average value of the argument |
| COUNT() | Return the number of records |
| COUNT_DISTINCT()) | Return the number of different values |
| MAX() | Return the maximum value |
| MIN() | Return the minimum value |
| STD() | Return the population standard deviation |
| SUM() | Return the sum |
| BIT_AND()      |   Bitwise AND |
| BIT_OR()        |   Bitwise OR |
| BIT_XOR()     |   Bitwise exclusive OR (XOR) |

All the functions above can only applies for int64 and double.

## Example

```ngql
nebula> GO FROM 1 OVER e1 YIELD $-.id AS fid | GROUP BY $-.fid YIELD COUNT(*)
-- for each fid, return the occurrence count.

nebula> GO FROM 1 YIELD e1._dst AS fid, e1.prop1 AS prop1 | GROUP BY $-.fid YIELD SUM($-.prop1)
-- for each fid, return the sum of prop1.
```

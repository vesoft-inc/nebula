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

All the functions above only work with int64 and double.

## Example

```ngql
nebula> GO FROM 100 OVER follow YIELD $$.player.name as Name | GROUP BY $-.Name YIELD $-.Name, COUNT(*);
-- Find all the players followed by vertex 100 and return their names as Name. These players are grouped by Name and the number in each group is counted.
-- The following result is returned:
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
-- Find all the players who follow vertex 101, return these players as player and the property of the follow edge as degree. These players are grouped and the sum of their degree values is returned.
-- The following result is returned:
==================
| SUM($-.degree) |
==================
| 186            |
------------------
```

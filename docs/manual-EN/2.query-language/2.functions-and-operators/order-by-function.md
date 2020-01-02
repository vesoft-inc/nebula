# Order by Function

Similar with SQL, `ORDER BY` can be used to sort in ascending (`ASC`) or descending (`DESC`) order for returned results.
And it can only be used in the `PIPE`-syntax (`|`).

```ngql
ORDER BY <expression> [ASC | DESC] [, <expression> [ASC | DESC] ...]
```

By default, `ORDER BY` sorts the records in ascending order if no `ASC` or `DESC` is given.

## Example

```ngql
nebula> FETCH PROP ON player 1,2,3,4 YIELD player.age AS age, player.weight AS weight | ORDER BY $-.age, $-.weight DESC  
-- fetch four vertices and sort them by their age in ascending order, and for those in the same age, sort them by weight in descending order.
```

(see [FETCH](../4.statement-syntax/2.data-query-and-manipulation-statements/fetch-syntax.md) for the usage)

```ngql
nebula> GO FROM 1 OVER edge2 YIELD $^.t1.prop1 AS s1_p1, edge2.prop2 AS e2_p2, $$.t3.prop3 AS d3_p3 | ORDER BY s1_p1 ASC, e2_p2 DESC, d3_p3 ASC

-- The return value of the above query  
   ==========================
   | s1_p1  | e2_p2 | d3_p3 |
   --------------------------
   |   123  |  345  |  234  |
   |   234  |  32   |   0   |
   |   234  |  31   |   0   |
   |   234  |  31   |   1   |
   ==========================
   The first column is sorted in ascending order, the second in descending order, and the third in ascending order.
```

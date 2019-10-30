# Set Operations (`UNION`, `INTERSECT`, and `MINUS`)

## `UNION`, `UNION DISTINCT`, and `UNION ALL`

Operator `UNION DISTINCT` (or by short `UNION`) returns the union of two sets A and B (denoted by `A ⋃ B` in mathematics), with the distinct element belongs to set A or set B, or both.

Meanwhile, operation `UNION ALL` returns the union set with duplicated elements. The `UNION`-syntax is

```
<left> UNION [DISTINCT | ALL] <right>
```

where `<left>` and `<right>` must have the same number of columns and pair-wise data types.

### Example

The following statement

```
GO FROM 1 OVER e1 \
UNION \
GO FROM 2 OVER e1
```

return the neighbors' id of vertex `1` and `2` (along with edge `e1`) without duplication.

While

```
GO FROM 1 OVER e1 \
UNION ALL\
GO FROM 2 OVER e1
```

returns all the neighbors of vertex `1` and `2`, with all possible duplications.

`UNION` can also work with the `YIELD` statement. For example, let's suppose the results of the following two queries 

```
nebula> GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left.1, $$.tag.prop2 AS left.2 -- query 1
==========================
| id  | left.1 | left.2  |
==========================
| 104 |    1   |    2    |    -- line 1
--------------------------
| 215 |    4   |    3    |    -- line 3
--------------------------

nebula> GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right.1, $$.tag.prop2 AS right.2  -- query 2
===========================
| id  | right.1 | right.2 |
===========================
| 104 |    1    |    2    |    -- line 1
---------------------------
| 104 |    2    |    2    |    -- line 2
---------------------------
```

And the following statement

```
GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left.1, $$.tag.prop2 AS left.2
UNION /* DISTINCT */
GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right.1, $$.tag.prop2 AS right.2
```

will return as follows:
```
=========================
| id  | left.1 | left.2 |    -- UNION or UNION DISTINCT. The column names come from query 1
=========================
| 104 |    1   |    2   |    -- line 1
-------------------------
| 104 |    2   |    2   |    -- line 2
-------------------------
| 215 |    4   |    3   |    -- line 3
-------------------------
```

Notice that line 1 and line 2 return the same id (104) with different column values. The `DISTINCT` check duplication by all the columns for every line. So line 1 and line 2 are different.

You can expect the `UNION ALL` result

```
GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left.1, $$.tag.prop2 AS left.2
UNION ALL
GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right.1, $$.tag.prop2 AS right.2

=========================
| id  | left.1 | left.2 |    -- UNION ALL
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

Operator `INTERSECT` returns the intersection of two sets A and B (denoted by A ⋂ B), if the elements belong both to set A and set B.

```
<left> INTERSECT <right>
```
Alike `UNION`, `<left>` and `<right>` must have the same number of columns and data types.
Besides, only the same line of `<left>` and `<right>` will be returned.

### Example

You can imagine

```
GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left.1, $$.tag.prop2 AS left.2
INTERSECT
GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right.1, $$.tag.prop2 AS right.2
```

will return

```
=========================
| id  | left.1 | left.2 |
=========================
| 104 |    1   |    2   |    -- line 1
-------------------------
```

## MINUS

The set subtraction (or difference), A - B, consists of elements that are in A but not in B. So the operation order matters.

### Example


```
GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left.1, $$.tag.prop2 AS left.2
MINUS
GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right.1, $$.tag.prop2 AS right.2
```

comes out

```
==========================
| id  | left.1 | left.2  |
==========================
| 215 |    4   |    3    |     -- line 3
--------------------------
```

And if we reverse the `MINUS` order

```
GO FROM 2,3 OVER e1 YIELD e1._dst AS id, e1.prop1 AS right.1, $$.tag.prop2 AS right.2
MINUS
GO FROM 1 OVER e1 YIELD e1._dst AS id, e1.prop1 AS left.1, $$.tag.prop2 AS left.2
```

results in

```
===========================
| id  | right.1 | right.2 |    -- column named from query 2
===========================
| 104 |    2    |    2    |    -- line 2
---------------------------
```


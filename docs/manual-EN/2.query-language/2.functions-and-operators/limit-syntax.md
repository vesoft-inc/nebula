# Limit Syntax

`LIMIT` works the same as in `SQL`, and must be used with pipe `|`. The `LIMIT` clause accepts one or two arguments. The values of both arguments must be zero or positive integers.

```ngql
ORDER BY <expressions> [ASC | DESC]
LIMIT [<offset_value>,] <number_rows>
```

* **expressions**

    The columns or calculations that you wish to sort.

* **number_rows**

    It constrains the number of rows to return. For example, LIMIT 10 would return the first 10 rows. This is where sorting order matters so be sure to use an `ORDER BY` clause appropriately.

* **offset_value**

    Optional. It defines from which row to start including the rows in the output. The offset starts from zero.

> When using `LIMIT`, it is important to use an `ORDER BY` clause that constrains the output into a unique order. Otherwise, you will get an unpredictable subset of the output.

For example:

```ngql
nebula> GO FROM 105 OVER follow YIELD $$.player.name AS Friend, $$.player.age AS Age, follow._dst AS Follow | LIMIT 2
================================
| Friend          | Age | Follow |
================================
| Tim Duncan      | 42  | 100  |
--------------------------------
| Marco Belinelli | 32  | 104  |
--------------------------------
```

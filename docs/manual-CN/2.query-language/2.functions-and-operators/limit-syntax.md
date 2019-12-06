# LIMIT 语法

`LIMIT` 用法与 `SQL` 中的相同，且只能与 `|` 结合使用。 `LIMIT` 子句接受一个或两个参数,两个参数的值都必须是零或正整数。

```ngql
ORDER BY <expressions> [ASC | DESC]
LIMIT [<offset_value>,] <number_rows>
```

* **expressions**

    待排序的列或计算。

* **number_rows**

    _number_rows_ 指定返回结果行数。例如，LIMIT 10 返回前 10 行结果。由于排序顺序会影响返回结果，所以使用 `ORDER BY` 时请注意排序顺序。

* **offset_value**

    可选选项，用来跳过指定行数返回结果，offset 从 0 开始。

> 当使用 `LIMIT` 时，请使用 `ORDER BY` 子句对返回结果进行唯一排序。否则，将返回难以预测的子集。

例如：

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

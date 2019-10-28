# LIMIT 语法

`LIMIT` 用法与 `SQL` 中的相同。

```sql
ORDER BY <expressions> [ASC | DESC]
LIMIT [<offset_value>,] <number_rows>
```

* **expressions**

    待排序的列或计算。

* **number_rows**

    _number_rows_指定返回结果行数。例如， LIMIT 10 返回前 10 行结果。请合理使用 `ORDER BY` 对返回结果进行排序。

* **offset_value**

    可选选项，用来跳过指定行数返回结果，offset 从 0 开始。

> 当使用 `LIMIT` 时，请使用 `ORDER BY` 子句对返回结果进行唯一排序，这点很重要。否则，将返回难以预测的子集。

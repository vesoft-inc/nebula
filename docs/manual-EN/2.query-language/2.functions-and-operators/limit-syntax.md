# Limit Syntax

`limit` works the same as in `SQL`.

```ngql
ORDER BY <expressions> [ASC | DESC]
LIMIT [<offset_value>,] <number_rows>
```

* **expressions**

    The columns or calculations that you wish to sort.

* **number_rows**

    It specifies a limited number of rows in the result set to be returned based on _number_rows_. For example, LIMIT 10 would return the first 10 rows. This is where sort order matters so be sure to use an `ORDER BY` clause appropriately.

* **offset_value**

    Optional. It says to skip that many rows before the first row returned. The offset starts from zero.

> When using `LIMIT`, it is important to use an `ORDER BY` clause that constrains the result rows into a unique order. Otherwise you will get an unpredictable subset of the query's rows.

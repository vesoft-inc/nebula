
```
YIELD
    [DISTINCT]
    <col_name> [AS <col_alias>]
    [, <col_name> [AS <col_alias>] ...]
```

* YIELD语句可以独立使用，而无需遍历图数据。您可以使用`AS`重命名返回的列。

```
nebula> YIELD 1 + 1
=========
| (1+1) |
=========
| 2     |
---------

nebula> YIELD "Hel" + "\tlo" AS HELLO_1, ", World!" as WORLD_2
======================
| HELLO_1 | WORLD_2  |
======================
| Hel   lo  | , World! |
----------------------
```

* 但是YIELD语句更常用于返回由`GO`（详情请参阅`GO`用法）语句生成的结果。


```
nebula> GO FROM 201 OVER relations_edge YIELD $$.student.name AS Friend, $$.student.age AS Age, $$.student.gender AS Gender
=========================
| Friend | Age | Gender |
=========================
|   Jane |  17 | female |
-------------------------
```

e.g., $$.student.name用来获取目标点（$$)的属性。

* DISTINCT

`YIELD DISTINCT`必须与`GO`同时使用
```
nebula> YIELD DISTINCT 1     --- 语法错误
```

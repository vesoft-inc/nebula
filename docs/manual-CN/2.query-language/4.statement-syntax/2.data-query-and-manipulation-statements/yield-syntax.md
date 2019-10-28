# YIELD 子句、语法

YIELD 关键字可以在FETCH、GO语法中作为子句使用，也可以在PIPE中作为独立的语法使用，同时可以作为用于计算的单句使用。

## 作为子句

```sql
YIELD
    [DISTINCT]
    <col_name> [AS <col_alias>]
    [, <col_name> [AS <col_alias>] ...]
```

常用于返回由`GO`（详情请参阅`GO`用法）语法生成的结果。

```SQL
nebula> GO FROM 201 OVER relations_edge YIELD $$.student.name AS Friend, \
  $$.student.age AS Age, $$.student.gender AS Gender
=========================
| Friend | Age | Gender |
=========================
|   Jane |  17 | female |
-------------------------
```

e.g., `$$.student.name` 用来获取目标点（$$)的属性。

## 作为语法

### 引用输入或者变量

- 可以在PIPE中使用YIELD语法。
- 可以用于引用变量。
- 对于那些不支持YIELD子句的语法，可以使用YIELD语法作为一个工具，控制输出。

```sql
YIELD
    [DISTINCT]
    <col_name> [AS <col_alias>]
    [, <col_name> [AS <col_alias>] ...]
    [WHERE <conditions>]
```

```SQL
nebula> GO FROM 201 OVER like YIELD like._dst AS id | YIELD $-.* WHERE $-.id == 200;

=========
| $-.id |
=========
| 200   |
---------

nebula> $var2 = GO FROM 200 OVER like;$var1 = GO FROM 201 OVER like;YIELD $var1.* UNION YIELD $var2.*;

===================
| $var1.like._dst |
===================
| 200             |
-------------------
| 201             |
-------------------
| 202             |
-------------------
```

### 作为独立的语法

- YIELD语法可以独立使用，用于一些简单的计算。您可以使用`AS`重命名返回的列。

```sql
nebula> YIELD 1 + 1
=========
| (1+1) |
=========
| 2     |
---------

nebula> YIELD "Hel" + "\tlo" AS HELLO_1, ", World!" AS WORLD_2
======================
| HELLO_1 | WORLD_2  |
======================
| Hel	lo  | , World! |
----------------------

nebula> YIELD hash("Tim") % 100
=====================
| (hash("Tim")%100) |
=====================
| 42                |
---------------------

```

## 注意事项

* DISTINCT

不支持`YIELD DISTINCT` 在单句中使用

```SQL
nebula> YIELD DISTINCT 1     --- 语法错误
```

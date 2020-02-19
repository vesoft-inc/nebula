# YIELD 子句、语句

`YIELD` 关键词可以在 `FETCH`、`GO` 语句中作为子句使用，也可以在 `PIPE`(`|`) 中作为独立的语句使用，同时可以作为用于计算的单句使用。

## 作为子句

```ngql
YIELD
    [DISTINCT]
    <col_name> [AS <col_alias>]
    [, <col_name> [AS <col_alias>] ...]
```

常用于返回由 `GO`（详情请参阅 [GO](go-syntax.md) 用法）语句生成的结果。

```ngql
nebula> GO FROM 100 OVER follow YIELD $$.player.name AS Friend, $$.player.age AS Age;
===========================
| Friend            | Age |
===========================
| Tony Parker       | 36  |
---------------------------
| LaMarcus Aldridge | 33  |
---------------------------
| Kyle Anderson     | 25  |
---------------------------
```

例如， `$$.player.name` 用来获取目标点（$$）的属性。

## 作为语句

### 引用输入或者变量

- 可以在 `PIPE` 中使用 `YIELD` 语句。
- 可以用于引用变量。
- 对于那些不支持 `YIELD` 子句的语句，可以使用 `YIELD` 语句作为一个工具，控制输出。

```ngql
YIELD
    [DISTINCT]
    <col_name> [AS <col_alias>]
    [, <col_name> [AS <col_alias>] ...]
    [WHERE <conditions>]
```

```ngql
nebula> GO FROM 100 OVER follow YIELD follow._dst AS id | YIELD $-.* WHERE $-.id == 106;

=========
| $-.id |
=========
| 106   |
---------

nebula> $var1 = GO FROM 101 OVER follow; $var2 = GO FROM 105 OVER follow; YIELD $var1.* UNION YIELD $var2.*;

=====================
| $var1.follow._dst |
=====================
| 100               |
---------------------
| 102               |
---------------------
| 104               |
---------------------
| 110               |
---------------------
```

### 作为独立的语句

- YIELD 语句可以独立使用，用于一些简单的计算。您可以使用 `AS` 重命名返回的列。

```ngql
nebula> YIELD 1 + 1;
=========
| (1+1) |
=========
| 2     |
---------

nebula> YIELD "Hel" + "\tlo" AS HELLO_1, ", World!" AS WORLD_2;
======================
| HELLO_1 | WORLD_2  |
======================
| Hel  lo  | , World! |
----------------------

nebula> YIELD hash("Tim") % 100;
=====================
| (hash("Tim")%100) |
=====================
| 42                |
---------------------

```

**注意：** 不支持 `YIELD DISTINCT` 在单句中使用。

```ngql
nebula> YIELD DISTINCT 1;     --- 语法错误
```

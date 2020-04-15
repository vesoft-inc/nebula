# Yield Clause and Statement

 Keyword `YIELD` can be used as a clause in a `FETCH` or `GO` statement, or as a separate statement in `PIPE`(`|`), or as a stand-alone statement for calculation.

## As Clause (With GO-Syntax)

```ngql
YIELD
    [DISTINCT]
    <col_name> [AS <col_alias>]
    [, <col_name> [AS <col_alias>] ...]
```

`YIELD` is commonly used to return results generated with `GO` (Refer [GO](go-syntax.md)).

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

For example: `$$.player.name` is used to get the property of the dest vertex ($$).

## As Statement

### Reference Inputs or Variables

- You can use the `YIELD` statement in `PIPE`.
- You can use the `YIELD` statement to reference variables.
- For statements that do not support `YIELD` statement, you can use it as a tool to control the output.

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

### As Stand-alone Statement

`YIELD` statement can be used independently to retrieve computation results without reference to any graph. You can use `AS` to rename it an alias.

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

**Note:** You can not use `YIELD DISTINCT` as a stand-alone statement. The following is a syntax error.

```ngql
nebula> YIELD DISTINCT 1     --- syntax error!
```

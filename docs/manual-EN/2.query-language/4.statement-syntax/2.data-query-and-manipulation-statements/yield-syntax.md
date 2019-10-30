# Yield Clause and Statement

 Keyword `YIELD` can be used as a clause in a `FETCH` or `GO` statement, or as a separate statement in `PIPE`, or as a single sentence for calculation.

## As Clause (with GO-syntax)

```sql
YIELD
    [DISTINCT]
    <col_name> [AS <col_alias>]
    [, <col_name> [AS <col_alias>] ...]
```

`YIELD` is commonly used to return results generated with `GO`.

```sql
nebula> GO FROM 201 OVER relations_edge YIELD $$.student.name AS Friend,\
        $$.student.age AS Age, $$.student.gender AS Gender
=========================
| Friend | Age | Gender |
=========================
|   Jane |  17 | female |
-------------------------
```

For example: `$$.student.name` is used to get the properties of the dest vertex ($$).

## As Statement

### Reference Inputs or Variables

- You can use the `YIELD` statement in `PIPE`.
- You can use the `YIELD` statement to reference variables.
- For statements that do not support `YIELD` statement, you can use it as a tool to control the outputs.

```sql
YIELD
    [DISTINCT]
    <col_name> [AS <col_alias>]
    [, <col_name> [AS <col_alias>] ...]
    [WHERE <conditions>]
```

```sql
nebula> GO FROM 201 OVER like YIELD like._dst AS id | YIELD $-.* WHERE $-.id == 200;

=========
| $-.id |
=========
| 200   |
---------

nebula> $var2 = GO FROM 200 OVER like;$var1 = GO FROM 201 OVER like; \
    YIELD $var1.* UNION YIELD $var2.*;

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

### As Separate Statement

`YIELD` statement can be used independently to retrieve computation results without reference to any graph. You can use `AS` to rename an alias.

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
| Hel  lo  | , World! |
----------------------

nebula> YIELD hash("Tim") % 100
=====================
| (hash("Tim")%100) |
=====================
| 42                |
---------------------
```

**Note**

You can not use `YIELD DISTINCT` as a separate statement. This is a syntax error.

```sql
nebula> YIELD DISTINCT 1     --- syntax error!
```

# Logical Operators

|  ***Name***    |  ***Description***    |
|:----|:----:|
|   &&     |   Logical AND     |
|   !      |   Logical NOT     |
|   \|\|   |   Logical OR   |
|   XOR      |   Logical XOR  |

In nGQL, nonzero numbers are evaluated to _true_. The precedence of the operators refer to [Operator Precedence](./operator-precedence.md).

* &&

Logical AND:

```sql
nebula> YIELD -1 && true
================
| (-(1)&&true) |
================
|true |
----------------
```

* !

Logical NOT:

```sql
nebula> YIELD !(-1)
===========
| !(-(1)) |
===========
|false |
-----------

```

* ||

Logical OR:

```sql
nebula> YIELD 1 || !1
=============
| (1||!(1)) |
=============
| true |
```

* ^

Logical XOR:

```sql
nebula> YIELD (NOT 0 || 0) AND 0 XOR 1 AS ret
=========
|  ret  |
=========
|   1   |
```

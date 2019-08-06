|  Name    |  Description    | 
|:----|:----:|
|   &&     |   Logical AND     | 
|   !    |   Logical NOT     | 
|   \|\|   |   Logical OR   | 


In nGQL, nonzero numbers are evaluated to _true_. The precedence of the operators refer to [Operator Precedence](./Operator_Precedence.md).

* &&

Logical AND:

```
(user@127.0.0.1) [(none)]> YIELD -1 && true;
================
| (-(1)&&true) |
================
|true |
----------------
```

* !

Logical NOT:

```
(user@127.0.0.1) [(none)]> YIELD !(-1);
===========
| !(-(1)) |
===========
|false |
-----------

```

* ||

Logical OR:

```
(user@127.0.0.1) [(none)]> YIELD 1 || !1;
=============
| (1||!(1)) |
=============
| true |
```
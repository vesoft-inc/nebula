# 逻辑运算符

|  ***名称***    |  ***描述***    |
|:----|:----:|
|   &&     |   逻辑与 AND     |
|   !      |   逻辑非 NOT     |
|   \|\|   |   逻辑或 OR   |
|  XOR |    逻辑异或 XOR|

在 nGQL 中，非 0 数字将被视为 _true_ 。逻辑运算符的优先级参见 [Operator Precedence](./operator-precedence.md)。

* &&

逻辑与 AND:

```ngql
nebula> YIELD -1 && true;
================
| (-(1)&&true) |
================
| true         |
----------------
```

* !

逻辑非 NOT:

```ngql
nebula> YIELD !(-1);
===========
| !(-(1)) |
===========
| false   |
-----------

```

* ||

逻辑或 OR:

```ngql
nebula> YIELD 1 || !1;
=============
| (1||!(1)) |
=============
| true      |
```

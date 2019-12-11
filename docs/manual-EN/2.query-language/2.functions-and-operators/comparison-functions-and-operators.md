# Comparison Functions and Operators

| Name  | Description |
|:----|:----:|
| =   | Assign a value   |
| /   | Division operator   |
| ==   | Equal operator   |
| !=   | Not equal operator   |
| <   | Less than operator   |
| <=   | Less than or equal operator   |
| -   | Minus operator   |
| %   | Modulo operator   |
| +   | Addition operator   |
| *   | Multiplication operator   |
| -   | Change the sign of the argument   |
|udf_is_in() | Whether a value is within a set of values |

Comparison operations result in a value of _true_ and _false_.

* ==

Equal. String comparisons are case-sensitive. Values of different type are not equal.

```ngql
nebula> YIELD 'A' == 'a';
==============
| ("A"=="a") |
==============
| false |
--------------

nebula> YIELD '2' == 2;
[ERROR (-8)]: A string type can not be compared with a non-string type.
```

* &gt;

Greater than：

```ngql
nebula> YIELD 3 > 2;
=========
| (3>2) |
=========
| true |
---------
```

* &ge;

Greater than or equal:

```ngql
nebula> YIELD 2 >= 2;
[ERROR (-8)]: A string type can not be compared with a non-string type.
```

* &lt;

Less than:

```ngql
nebula> YIELD 2.0 < 1.9;
=======================
| (2.000000<1.900000) |
=======================
|false |
-----------------------
```

* &le;

Less than or equal:

```ngql
nebula> YIELD 0.11 <= 0.11;
========================
| (0.110000<=0.110000) |
========================
|true |
------------------------
```

* !=

Not equal:

```ngql
nebula> YIELD 1 != '1'
A string type can not be compared with a non-string type.
```

* udf_is_in()

Returns true if the first value is equal to any of the values in the  list, else returns false.

```ngql
nebula> YIELD udf_is_in(1,0,1,2)
======================
| udf_is_in(1,0,1,2) |
======================
| true               |
----------------------

nebula> GO FROM 201 OVER like WHERE udf_is_in($$.student.name, "Jane")
=============
| like._dst |
=============
| 202       |
-------------

nebula> GO FROM 201 OVER like YIELD like._dst AS id | GO FROM $-.id OVER like WHERE udf_is_in($-.id, 200, 200+1)
=============
| like._dst |
=============
| 201       |
```

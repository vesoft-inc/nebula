# 比较函数和运算符

| 运算符  | 描述 |
|:----  |:----:|
| =     | 赋值运算符   |
| /     | 除法运算符  |
| ==    | 等于运算符  |
| !=    | 不等于运算符  |
| <     | 小于运算符   |
| <=    | 小于或等于运算符   |
| -     | 减法运算符   |
| %     | 余数运算符   |
| +     | 加法运算符   |
| *     | 乘法运算符  |
| -     | 负号运算符   |
| udf_is_in() | 比较函数，判断值是否在指定的列表中 |

比较运算的结果是 _true_ 或 _false_ 。

* ==

等于。String的比较大小写敏感。不同类的值不相同：

```ngql
nebula> YIELD 'A' == 'a';
==============
| ("A"=="a") |
==============
| false      |
--------------

nebula> YIELD '2' == 2;
[ERROR (-8)]: A string type can not be compared with a non-string type.
```

* &gt;

大于：

```ngql
nebula> YIELD 3 > 2;
=========
| (3>2) |
=========
| true  |
---------
```

* &ge;

大于或等于：

```ngql
nebula> YIELD 2 >= 2;
==========
| (2>=2) |
==========
| true   |
----------
```

* &lt;

小于：

```ngql
nebula> YIELD 2.0 < 1.9;
=======================
| (2.000000<1.900000) |
=======================
| false               |
-----------------------
```

* &le;

小于或等于：

```ngql
nebula> YIELD 0.11 <= 0.11;
========================
| (0.110000<=0.110000) |
========================
| true                 |
------------------------
```

* !=

不等于：

```ngql
nebula> YIELD 1 != '1'
[ERROR (-8)]: A string type can not be compared with a non-string type.
```

* udf_is_in()

第一个参数为要比较的值。

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
-------------
```

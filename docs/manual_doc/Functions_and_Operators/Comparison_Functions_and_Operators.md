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


Comparison operations result in a value of _true_ and _false_.

* ==

Equal. String comparisons are case-sensitive. Values of different type are not equal.

```
(user@127.0.0.1) [(none)]> YIELD 'A' == 'a';
==============
| ("A"=="a") |
==============
| false |
--------------

(user@127.0.0.1) [(none)]> YIELD '2' == 2;
============
| ("2"==2) |
============
|false |
------------
```

* &gt;

Greater than： 

```
(user@127.0.0.1) [(none)]> YIELD 3 > 2;
=========
| (3>2) |
=========
| true |
---------
```

* &ge;

Greater than or equal:

```
(user@127.0.0.1) [(none)]> YIELD 2 >= 2;
==========
| (2>=2) |
==========
| true |
----------
```

* &lt;

Less than:

```
(user@127.0.0.1) [(none)]> YIELD 2.0 < 1.9;
=======================
| (2.000000<1.900000) |
=======================
|false |
-----------------------
```

* &le;

Less than or equal:

```
(user@127.0.0.1) [(none)]> YIELD 0.11 <= 0.11;
========================
| (0.110000<=0.110000) |
========================
|true |
------------------------
```

* !=

Not equal:

```
(user@127.0.0.1) [(none)]> YIELD 1 != '1'
============
| (1!="1") |
============
| true |
------------
```
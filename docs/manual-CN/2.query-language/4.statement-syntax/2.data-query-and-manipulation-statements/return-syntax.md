# Return 语法

Return 语句用于返回条件成立时的结果。如果条件不成立，则无返回结果。

```ngql
    RETURN <var_ref> IF <var_ref> IS NOT NULL
```

* <var_ref> 为变量名称，示例：$var

## 示例

```ngql
nebula> $A = GO FROM 100 OVER follow YIELD follow._dst AS dst; \
        $rA = YIELD $A.* WHERE $A.dst == 101; \
        RETURN $rA IF $rA is NOT NULL; \ /* $rA 为非空，返回 $rA */
        GO FROM $A.dst OVER follow; /* 因为 RETURN 语句返回了结果，所以GO FROM 语句不执行 */
==========
| $A.dst |
==========
| 101    |
----------
nebula> $A = GO FROM 100 OVER follow YIELD follow._dst AS dst; \
        $rA = YIELD $A.* WHERE $A.dst == 300; \
        RETURN $rA IF $rA is NOT NULL; \ /* $rA 为空，不返回任何值 */
        GO FROM $A.dst OVER follow; /* 因为 RETURN 语句无返回结果，所以 GO FROM 语句将执行 */
===============
| follow._dst |
===============
| 100         |
---------------
| 101         |
---------------
| 100         |
---------------
| 102         |
---------------
| 100         |
---------------
| 107         |
---------------
```

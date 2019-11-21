# Return 语法

Return 语句用于返回条件成立时的结果。

```ngql
    RETURN <var_ref> IF <var_ref> IS NOT NULL
```

* <var_ref> 为变量名称，示例：$var

## 示例

```ngql
nebula> $A = GO FROM 200 OVER follow YIELD follow._dst AS dst; \
        $rA = YIELD $A.* WHERE $A.dst == 201; \
        RETURN $rA IF $rA is NOT NULL; \ /* $rA 为非空，返回 $rA */
        GO FROM $A.dst OVER follow; /* 语句不执行*/
==========
| $A.dst |
==========
| 201    |
----------
nebula> $A = GO FROM 200 OVER follow YIELD follow._dst AS dst; \
        $rA = YIELD $A.* WHERE $A.dst == 300; \
        RETURN $rA IF $rA is NOT NULL; \ /* $rA 为空，不返回任何值 */
        GO FROM $A.dst OVER follow;
=============
| follow._dst |
=============
| 200       |
-------------
| 202       |
-------------
```

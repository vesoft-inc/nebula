# Return 语法

Return 语句用于返回条件成立时的结果。

```ngql
    RETURN <var_ref> IF <var_ref> IS NOT NULL
```

* <var_ref> 变量名称， 示例： $var

# 示例

```
(user@127.0.0.1) [myspace_test2]> $A = GO FROM 200 OVER like YIELD like._dst AS dst; \
                               -> $rA = YIELD $A.* WHERE $A.dst == 201; \
                               -> RETURN $rA IF $rA is NOT NULL; \ /* return here since $rA is not empty */
                               -> GO FROM $A.dst OVER like; /* will never be executed*/
==========
| $A.dst |
==========
| 201    |
----------

(user@127.0.0.1) [myspace_test2]> $A = GO FROM 200 OVER like YIELD like._dst AS dst; \
                               -> $rA = YIELD $A.* WHERE $A.dst == 300; \
                               -> RETURN $rA IF $rA is NOT NULL; \ /* not return since $rA is empty */
                               -> GO FROM $A.dst OVER like;
=============
| like._dst |
=============
| 200       |
-------------
| 202       |
-------------
```
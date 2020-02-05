# Return Syntax

The `RETURN` statement is used to return the result when the condition is true. If the condition is false, no result is returned.

```ngql
    RETURN <var_ref> IF <var_ref> IS NOT NULL
```

* <var_ref> is a variable name, e.g. `$var`.

## Examples

```ngql
nebula> $A = GO FROM 100 OVER follow YIELD follow._dst AS dst; \
        $rA = YIELD $A.* WHERE $A.dst == 101; \
        RETURN $rA IF $rA is NOT NULL; \ /* Returns the result because $rA is not empty */
        GO FROM $A.dst OVER follow; /* As the RETURN statement returns the result, the GO FROM statement is not executed*/
==========
| $A.dst |
==========
| 101    |
----------
nebula> $A = GO FROM 100 OVER follow YIELD follow._dst AS dst; \
        $rA = YIELD $A.* WHERE $A.dst == 300; \
        RETURN $rA IF $rA is NOT NULL; \ /* Does not return the result because $rA is empty */
        GO FROM $A.dst OVER follow; /* As the RETURN statement does not return the result, the GO FROM statement is executed */
=============
| follow._dst |
=============
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

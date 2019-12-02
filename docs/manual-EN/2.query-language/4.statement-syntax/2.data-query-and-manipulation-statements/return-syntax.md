# Return Syntax

The `RETURN` statement is used to return the results when the filer conditions are true.

```ngql
    RETURN <var_ref> IF <var_ref> IS NOT NULL
```

* <var_ref> is a variable name, e.g. `$var`.

## Examples

```ngql
nebula> $A = GO FROM 200 OVER follow YIELD follow._dst AS dst; \
        $rA = YIELD $A.* WHERE $A.dst == 201; \
        RETURN $rA IF $rA is NOT NULL; \ /* return here since $rA is not empty */
        GO FROM $A.dst OVER follow; /* will never be executed*/
==========
| $A.dst |
==========
| 201    |
----------
nebula> $A = GO FROM 200 OVER follow YIELD follow._dst AS dst; \
        $rA = YIELD $A.* WHERE $A.dst == 300; \
        RETURN $rA IF $rA is NOT NULL; \ /* not return since $rA is empty */
        GO FROM $A.dst OVER follow;
=============
| follow._dst |
=============
| 200       |
-------------
| 202       |
-------------
```

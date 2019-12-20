# Where Syntax

Currently, the `WHERE` statement only applies to the `GO` statement.

```ngql
WHERE <expression> [ AND | OR <expression> ...])  
```

Usually, `WHERE` is a set of logical combination that filters vertex or edge properties.

> As syntactic sugar, you can freely choose to use both `AND` and  `&&` as boolean logical and, `OR` and `||` as boolean logical or.

## Examples

```ngql
-- the edge e1's property prop1 is greater than 17
nebula> GO FROM 201 OVER e1 WHERE e1.prop1 >= 17
-- the source vertex v1's property prop1 is equivalent with dest vertex v2's property prop2
nebula> GO FROM 201 OVER e1 WHERE $^.v1.prop1 == $$.v2.prop2
-- logical combination is allowed
nebula> GO FROM 201 OVER e1 WHERE ((e3.prop3 < 0.5) \
   OR ($^.v4.prop4 != "hello")) AND $$.v5.prop5 == "world"
-- always TRUE
nebula> GO FROM 201 OVER e1 WHERE 1 == 1 OR TRUE
```

# Where Syntax

Currently, the `WHERE` statement only applies to the `GO` statement.

```
WHERE (expression [ AND | OR expression ...])  
```

Usually, the expression is for vertex or edge properties.

>As syntax sugar, you can freely choose to use both `AND` and `&&`. They are both boolean logical and. So do `OR` and `||`.

### Examples

```
/* GO FROM 201 OVER like */  -- Apply in a GO statement
WHERE e1.prop1 >= 17     -- the edge e1's property prop1 is larger than 17

WHERE $^.v1.prop1 == $$.v2.prop2  -- the source vertex v1's property prop1 is equivalent with destination vertex v2's property prop2

WHERE ((e3.prop3 < 0.5) OR ($^.v4.prop4 != "hello")) AND $$.v5.prop5 == "world"   -- logical combination is allowed

WHERE 1 == 1 OR TRUE    --always TRUE
```

### Reference

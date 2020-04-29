# Where Syntax

Currently, the `WHERE` statement only applies to the `GO` statement.

```ngql
WHERE <expression> [ AND | OR <expression> ...])  
```

Usually, `WHERE` is a set of logical combination that filters vertex or edge properties.

> As syntactic sugar, logic AND is represented by `AND` or `&&` and logic OR is represented by `OR` or `||`.

## Examples

```ngql
-- the degree property of edge follow is greater than 90.
nebula> GO FROM 100 OVER follow WHERE follow.degree > 90;
-- the following result is returned:
===============
| follow._dst |
===============
| 101         |
---------------
-- find the dest vertex whose age is equal to the source vertex, player 104.
nebula> GO FROM 104 OVER follow WHERE $^.player.age == $$.player.age;
-- the following result is returned:
===============
| follow._dst |
===============
| 103         |
---------------
-- logical combination is allowed.
nebula> GO FROM 100 OVER follow WHERE follow.degree > 90 OR $$.player.age != 33 AND $$.player.name != "Tony Parker";
-- the following result is returned:
===============
| follow._dst |
===============
| 101         |
---------------
| 106         |
---------------
-- the condition in the WHERE clause is always TRUE.
nebula> GO FROM 101 OVER follow WHERE 1 == 1 OR TRUE;
-- the following result is returned:
===============
| follow._dst |
===============
| 100         |
---------------
| 102         |
---------------
```

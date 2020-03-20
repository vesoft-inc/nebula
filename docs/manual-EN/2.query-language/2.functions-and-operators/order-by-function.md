# Order by Function

Similar with SQL, `ORDER BY` can be used to sort in ascending (`ASC`) or descending (`DESC`) order for returned results.
 `ORDER BY` can only be used in the `PIPE`-syntax (`|`).

```ngql
ORDER BY <expression> [ASC | DESC] [, <expression> [ASC | DESC] ...]
```

By default, `ORDER BY` sorts the records in ascending order if no `ASC` or `DESC` is given.

## Example

```ngql
nebula> FETCH PROP ON player 100,101,102,103 YIELD player.age AS age, player.name AS name | ORDER BY age, name DESC;  

-- Fetch four vertices and sort them by their ages in ascending order, and for those in the same age, sort them by name in descending order.
-- The following result is returned:
======================================
| VertexID | age | name              |
======================================
| 103      | 32  | Rudy Gay          |
--------------------------------------
| 102      | 33  | LaMarcus Aldridge |
--------------------------------------
| 101      | 36  | Tony Parker       |
--------------------------------------
| 100      | 42  | Tim Duncan        |
--------------------------------------
```

(see [FETCH](../4.statement-syntax/2.data-query-and-manipulation-statements/fetch-syntax.md) for the usage)

```ngql
nebula> GO FROM 100 OVER follow YIELD $$.player.age AS age, $$.player.name AS name | ORDER BY age DESC, name ASC;

-- Search all the players followed by vertex 100 and return their ages and names. The age is in descending order; the name is in ascending order if they have the same name.
-- The following result is returned:  
===========================
| age | name              |
===========================
| 36  | Tony Parker       |
---------------------------
| 33  | LaMarcus Aldridge |
---------------------------
| 25  | Kyle Anderson     |
---------------------------
```

<!---
Not supported yet

If you compare a string with an int (especially in the `WHERE` and `YIELD` syntax), notice that there are implicit conversion from int to string.

```
# CREATE EDGE e1 (prop1 string)          -- create an edge with property of string
GO FROM 1 OVER e1 WHERE "prop1" > "12"   -- line 1: compare two strings "prop1" with "12" (which is always true)
GO FROM 1 OVER e1 WHERE e1.prop1 > "12"    -- line 2: reference the property of edge and compare with 12
GO FROM 1 OVER e1 WHERE prop1 > 12       -- line 3: Watch out! This may not what you want as line 2. This is the same as line 1
```


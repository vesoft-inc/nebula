# SHOW CREATE TAGS/EDGES Syntax

```ngql
SHOW CREATE {TAG <tag_name> | EDGE <edge_name>}
```

`SHOW CREATE TAG` and `SHOW CREATE EDGE` return the specified tag or edge type and their creation syntax in a given space. If the tag or edge type contains a default value, the default value is also returned.

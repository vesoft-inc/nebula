# Describe Syntax

```
DESCRIBE SPACE <space_name>
DESCRIBE TAG <tag_name>
DESCRIBE EDGE <edge_name>
```

The DESCRIBE keyword is used to obtain information about space, tag, edge structure.

Also notice that DESCRIBE is different from SHOW. Refer [SHOW](show-syntax.md).

## Example

Obtain information about space.

```SQL
nebula> DESCRIBE SPACE laura_space;
========================================================
| ID |        Name | Partition number | Replica Factor |
========================================================
|  1 | laura_space |             1024 |              1 |
--------------------------------------------------------  
```

Obtain information about tag.

```SQL
nebula> DESCRIBE TAG player
==================
| Field |   Type |
==================
|  name | string |
------------------
|   age |    int |
------------------  
```

Obtain information about edge.

```SQL
nebula> DESCRIBE EDGE serve
=====================
|      Field | Type |
=====================
| start_year |  int |
---------------------
|   end_year |  int |
---------------------
```

# Describe Syntax

```
DESCRIBE SPACE <space_name>
DESCRIBE TAG <tag_name>
DESCRIBE EDGE <edge_name>
```

The DESCRIBE keyword is used to obtain information about space, tag, edge structure.

Also notice that DESCRIBE is different from SHOW.

## Example

Providing information about space. <!-- As regards information about CREATE SPACE. -->

```SQL
nebula> DESCRIBE SPACE laura_space;
========================================================
| ID |        Name | Partition number | Replica Factor |
========================================================
|  1 | laura_space |             1024 |              1 |
--------------------------------------------------------  
```

Providing information about tag. <!-- As regards information about CREATE TAG. -->

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

Providing information about edge. <!-- As regards information about CREATE EDGE. -->

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

# Describe Syntax

```ngql
DESCRIBE SPACE <space_name>
DESCRIBE TAG <tag_name>
DESCRIBE EDGE <edge_name>
DESCRIBE {TAG | EDGE} INDEX <index_name>
```

The DESCRIBE keyword is used to obtain information about space, tag, edge structure.

Also notice that DESCRIBE is different from SHOW. Refer [SHOW](show-syntax.md).

## Example

Obtain information about space.

```ngql
nebula> DESCRIBE SPACE nba;
========================================================
| ID |        Name | Partition number | Replica Factor |
========================================================
|  1 |     nba     |             100  |              1 |
--------------------------------------------------------  
```

Obtain information about tag in a given space.

```ngql
nebula> DESCRIBE TAG player
==================
| Field |   Type |
==================
|  name | string |
------------------
|   age |    int |
------------------  
```

Obtain information about edge in a given space.

```ngql
nebula> DESCRIBE EDGE serve
=====================
|      Field | Type |
=====================
| start_year |  int |
---------------------
|   end_year |  int |
---------------------
```

Obtain information about the index.

```ngql
nebula> DESCRIBE TAG INDEX player_index_0;
==================
| Field | Type   |
==================
| name  | string |
------------------
```

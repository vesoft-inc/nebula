
```
{DESCRIBE | DESC } SPACE space_name
{DESCRIBE | DESC } TAG tag_name
{DESCRIBE | DESC } EDGE edge_name
```

The DESCRIBE/DESC, and EXPLAIN statements are DIFFERENT in Nebula. The DESCRIBE/DESC keyword is used to obtain information about space, tag, edge structure. Meanwhile, EXPLAIN is used to obtain a query execution plan. Please refer to XXX for more details about EXPLAIN.

Also notice that DESCRIBE is different from SHOW. check XXX for SHOW.

### Examples

Providing information about space. As regards information about CREATE SPACE.

```
nebula> DESCRIBE SPACE laura_space;
========================================================
| ID |        Name | Partition number | Replica Factor |
========================================================
|  1 | laura_space |             1024 |              1 |
--------------------------------------------------------  
```

Providing information about tag. As regards information about CREATE TAG.

```
nebula> DESCRIBE TAG player
==================
| Field |   Type |
==================
|  name | string |
------------------
|   age |    int |
------------------  
```

Providing information about edge. As regards information about CREATE EDGE.

```
nebula> DESCRIBE EDGE serve
=====================
|      Field | Type |
=====================
| start_year |  int |
---------------------
|   end_year |  int |
---------------------
```

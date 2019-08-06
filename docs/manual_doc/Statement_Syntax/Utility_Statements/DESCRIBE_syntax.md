
```
{DESCRIBE | DESC } SPACE space_name
{DESCRIBE | DESC } TAG tag_name
{DESCRIBE | DESC } EDGE edge_name
```

The DESCRIBE/DESC, and EXPLAIN statements are DIFFERENT in Nebula. The DESCRIBE/DESC keyword is used to obtain information about space, tag, edge structure. Meanwhile, EXPLAIN is used to obtain a query execution plan. Please refer to XXX for more details about EXPLAIN.

Also notice that DESCRIBE is different from SHOW. check XXX for SHOW.

### Examples

Providing information about space. Please refer to XXX about CREATE SPACE.

```
(user@127.0.0.1) [(none)]> DESCRIBE SPACE laura_space;
========================================================
| ID |        Name | Partition number | Replica Factor |
========================================================
|  1 | laura_space |             1024 |              1 |
--------------------------------------------------------  
```

Providing information about tag. Please refer to XXX about CREATE TAG.

```
(user@127.0.0.1) [(none)]> DESCRIBE TAG player
==================
| Field |   Type |
==================
|  name | string |
------------------
|   age |    int |
------------------  
```

Providing information about edge. Please refer to XXX about CREATE EDGE.

```
(user@127.0.0.1) [(none)]> DESCRIBE EDGE serve
=====================
|      Field | Type |
=====================
| start_year |  int |
---------------------
|   end_year |  int |
---------------------
```

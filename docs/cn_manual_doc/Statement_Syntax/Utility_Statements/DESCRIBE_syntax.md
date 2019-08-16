# DESCRIBE

```
{DESCRIBE | DESC } SPACE space_name
{DESCRIBE | DESC } TAG tag_name
{DESCRIBE | DESC } EDGE edge_name
```

在 Nebula 中 DESCRIBE/DESC 和 EXPLAIN 是`不同`的关键词。 DESCRIBE/DESC 关键词的作用是获取关于 space, tag, edge 结构的信息。 然而 EXPLAIN 的作用是获取一个请求执行计划。 参见文档来获取更多有关 [EXPLAIN]() 的信息。

同时需要注意的是，DESCRIBE 和 SHOW 也是不同的。 详细参见 [SHOW]() 文档。

### 示例

提供关于 space 的信息，对应 CREATE SPACE。

```
nebula> DESCRIBE SPACE laura_space;
========================================================
| ID |        Name | Partition number | Replica Factor |
========================================================
|  1 | laura_space |             1024 |              1 |
--------------------------------------------------------  
```

提供关于 tag 的信息，对应 CREATE TAG。

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

提供关于 EDGE 的信息，对应 CREATE EDGE。

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

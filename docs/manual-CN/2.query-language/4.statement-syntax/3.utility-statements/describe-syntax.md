# DESCRIBE 语法

```
DESCRIBE SPACE space_name
DESCRIBE TAG tag_name
DESCRIBE EDGE edge_name
```

DESCRIBE 关键词的作用是获取关于 space, tag, edge 结构的信息。

同时需要注意的是，DESCRIBE 和 SHOW 也是不同的。 详细参见 [SHOW](show-syntax.md) 文档。

## 示例

获取指定 space 的信息，对应 `DESCRIBE SPACE`。

```SQL
nebula> DESCRIBE SPACE laura_space;
========================================================
| ID |        Name | Partition number | Replica Factor |
========================================================
|  1 | laura_space |             1024 |              1 |
--------------------------------------------------------  
```

获取指定 tag 的信息，对应 `DESCRIBE TAG`。

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

获取指定 EDGE 的信息，对应 `DESCRIBE EDGE`。

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

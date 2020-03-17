# DESCRIBE 语法

```ngql
DESCRIBE SPACE <space_name>
DESCRIBE TAG <tag_name>
DESCRIBE EDGE <edge_name>
DESCRIBE {TAG | EDGE} INDEX <index_name>
```

DESCRIBE 关键词的作用是获取关于 space, tag, edge 结构的信息。

同时需要注意的是，DESCRIBE 和 SHOW 也是不同的。 详细参见 [SHOW](show-syntax.md) 文档。

## 示例

获取指定 space 的信息，对应 `DESCRIBE SPACE`。

```ngql
nebula> DESCRIBE SPACE nba;
========================================================
| ID |     Name    | Partition number | Replica Factor |
========================================================
|  1 |     nba     |             100  |              1 |
--------------------------------------------------------  
```

获取指定 tag 的信息，对应 `DESCRIBE TAG`。

```ngql
nebula> DESCRIBE TAG player;
==================================================
| Field | Type   |  Null | Key | Default | Extra |
==================================================
| name  | string | false |     |         |       |
--------------------------------------------------
|  age  | int    | false |     |         |       |
--------------------------------------------------
```

获取指定 EDGE 的信息，对应 `DESCRIBE EDGE`。

```ngql
nebula> DESCRIBE EDGE serve;
======================================================
|      Field | Type |   Null | Key | Default | Extra |
======================================================
| start_year |  int |  false |     |         |       |
------------------------------------------------------
|   end_year |  int |  false |     |         |       |
------------------------------------------------------
```

返回指定索引信息，对应 `DESCRIBE INDEX`。

```ngql
nebula> DESCRIBE TAG INDEX player_index_0;
==================
| Field | Type   |
==================
| name  | string |
------------------
```

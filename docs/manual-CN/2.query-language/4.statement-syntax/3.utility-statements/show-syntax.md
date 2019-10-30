# SHOW 语法

```sql
SHOW {SPACES | TAGS | EDGES | HOSTS}
SHOW VARIABLES [graph|meta|storage]
```

`SHOW SPACES` 列出 Nebula Graph 集群中的所有图空间。

`SHOW TAGS` 和 `SHOW EDGES` 则返回当前图空间中被定义的 tag 和 edge type。

`SHOW HOSTS` 列出元服务器注册的所有存储主机。

更多关于 `SHOW VARIABLES [graph|meta|storage]` 的信息，variable syntax一节。

## 示例

```SQL
nebula> SHOW SPACES;
========
| Name |
========
| test |
--------

nebula> USE test;
nebula> SHOW TAGS;
nebula> SHOW EDGES;
```

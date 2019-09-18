# SHOW 语法

```
SHOW {SPACES | TAGS | EDGES | HOSTS}
SHOW VARIABLES [graph|meta|storage] 
```

`SHOW SPACES` 列出 Nebula Graph 集群中的所有图空间。

`SHOW TAGS` 和 `SHOW EDGES` 则返回当前图空间中被定义的 tag 和 edge type。

`SHOW HOSTS` 列出元服务器注册的所有存储主机。

更多关于 `SHOW VARIABLES [graph|meta|storage]` 的信息，请参见 [variable syntax](../data-administration-statements/configuration-statements/variables-syntax.md)。

### 示例

```
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
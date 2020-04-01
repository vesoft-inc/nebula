# SHOW PARTS 语法

```ngql
SHOW PARTS <part_id>
```

`SHOW PARTS` 列出指定 partition 的信息。

```ngql
nebula> SHOW PARTS 1;
==============================================================
| Partition ID | Leader           | Peers            | Losts |
==============================================================
| 1            | 172.28.2.2:44500 | 172.28.2.2:44500 |       |
--------------------------------------------------------------
```

`SHOW PARTS` 输出以下列：

- Partition ID
- Leader
- Peers
- Losts
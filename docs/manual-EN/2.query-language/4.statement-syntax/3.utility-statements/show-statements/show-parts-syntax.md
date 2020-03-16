# SHOW PARTS Syntax

```ngql
SHOW PARTS <part_id>
```

`SHOW PARTS` lists the partition information of the given SPACE.

```ngql
nebula> SHOW PARTS 1;
==============================================================
| Partition ID | Leader           | Peers            | Losts |
==============================================================
| 1            | 172.28.2.2:44500 | 172.28.2.2:44500 |       |
--------------------------------------------------------------
```

`SHOW PARTS` output has these columns:

- Partition ID
- Leader
- Peers
- Losts

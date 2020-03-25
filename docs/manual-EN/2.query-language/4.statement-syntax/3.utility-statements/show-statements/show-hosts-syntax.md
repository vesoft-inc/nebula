# SHOW HOSTS Syntax

```ngql
SHOW HOSTS
```

`SHOW HOSTS` statement lists storage hosts registered by the meta server. `SHOW HOSTS` output has these columns:: ip, port, status (online/offline), leader count, leader distribution, partition distribution.

```ngql
nebula> SHOW HOSTS;
=============================================================================================
| Ip         | Port  | Status | Leader count | Leader distribution | Partition distribution |
=============================================================================================
| 172.28.2.1 | 44500 | online | 0            | No valid partition  | No valid partition     |
---------------------------------------------------------------------------------------------
| 172.28.2.2 | 44500 | online | 2            | NBA: 1, gods: 1     | NBA: 1, gods: 1        |
---------------------------------------------------------------------------------------------
| 172.28.2.3 | 44500 | online | 0            | No valid partition  | No valid partition     |
---------------------------------------------------------------------------------------------
| Total      |       |        | 2            | gods: 1, NBA: 1     | gods: 1, NBA: 1        |
---------------------------------------------------------------------------------------------
```

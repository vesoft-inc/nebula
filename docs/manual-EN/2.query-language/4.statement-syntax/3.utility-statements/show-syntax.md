# Show Syntax

```ngql
SHOW {SPACES | TAGS | EDGES | HOSTS | PARTS}
SHOW CONFIGS [graph|meta|storage]
```

`SHOW SPACES` lists the SPACES on the **Nebula Graph** cluster.

`SHOW TAGS` and `SHOW EDGES` return the defined tags and edge types in a given space, respectively.

`SHOW HOSTS` is to list storage hosts registered by the meta server. There are 6 columns: ip, port, status (online/offline), leader partitions count in all spaces, leader partitions count in each space, total partitions count in all spaces.

`SHOW PARTS` lists the partition information of the given SPACE.

For more information about `SHOW CONFIGS [graph|meta|storage]`, please refer to [configs syntax](../../../3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/configs-syntax.md).

## Example

```ngql
nebula> SHOW SPACES;
========
| Name |
========
| test |
--------

nebula> USE test;
nebula> SHOW PARTS;
======================================================================================================
| Partition ID | Leader            | Peers                                                   | Losts |
======================================================================================================
| 1            | 172.25.61.1:44600 | 172.25.61.1:44700, 172.25.61.1:44500, 172.25.61.1:44600 |       |
------------------------------------------------------------------------------------------------------
| 2            | 172.25.61.1:44600 | 172.25.61.1:44500, 172.25.61.1:44600, 172.25.61.1:44700 |       |
------------------------------------------------------------------------------------------------------
| 3            | 172.25.61.1:44600 | 172.25.61.1:44600, 172.25.61.1:44700, 172.25.61.1:44500 |       |
------------------------------------------------------------------------------------------------------

nebula> SHOW TAGS;
=================
| ID | Name     |
=================
| 2  | course   |
-----------------
| 3  | building |
-----------------
| 4  | student  |
-----------------

nebula> SHOW EDGES;
===============
| ID | Name   |
===============
| 5  | follow |
---------------
| 6  | choose |
---------------
```

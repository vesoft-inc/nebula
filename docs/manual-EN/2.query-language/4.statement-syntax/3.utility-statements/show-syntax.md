# Show Syntax

```ngql
SHOW {SPACES | TAGS | EDGES | HOSTS | PARTS}
SHOW PART <part_id>
SHOW CREATE {TAG | EDGE} {<tag_name> | <edge_name>}
SHOW CONFIGS [graph|meta|storage]
```

`SHOW SPACES` lists the SPACES on the **Nebula Graph** cluster.

`SHOW TAGS` and `SHOW EDGES` return the defined tags and edge types in a given space, respectively.

`SHOW CREATE TAG` and `SHOW CREATE EDGE` return the specified tag or edge type and their creation syntax in a given space. If the tag or edge type contains a default value, the default value is also returned. 

`SHOW HOSTS` is to list storage hosts registered by the meta server. There are 6 columns: ip, port, status (online/offline), leader partitions count in all spaces, leader partitions count in each space, total partitions count in all spaces.

`SHOW PARTS` lists the partition information of the given SPACE.

`SHOW PART <part_id>` lists the specified partition information.

For more information about `SHOW CONFIGS [graph|meta|storage]`, please refer to [configs syntax](../../../3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/configs-syntax.md).

## Example

```ngql
nebula> SHOW HOSTS;
=============================================================================================
| Ip         | Port  | Status | Leader count | Leader distribution | Partition distribution |
=============================================================================================
| 172.28.2.1 | 44500 | online | 33           | nba: 3, test: 30    | test: 100, nba: 3      |
---------------------------------------------------------------------------------------------
| 172.28.2.2 | 44500 | online | 48           | nba: 4, test: 44    | test: 100, nba: 4      |
---------------------------------------------------------------------------------------------
| 172.28.2.3 | 44500 | online | 30           | nba: 3, test: 27    | test: 100, nba: 3      |
---------------------------------------------------------------------------------------------
| Total      |       |        | 111          | nba: 10, test: 101  | test: 300, nba: 10     |
---------------------------------------------------------------------------------------------

nebula> SHOW SPACES;
========
| Name |
========
| nba |
--------

nebula> USE nba;
nebula> SHOW PARTS;
==============================================================
| Partition ID | Leader           | Peers            | Losts |
==============================================================
| 1            | 172.28.2.2:44500 | 172.28.2.2:44500 |       |
--------------------------------------------------------------
| 2            | 172.28.2.3:44500 | 172.28.2.3:44500 |       |
--------------------------------------------------------------
| 3            | 172.28.2.1:44500 | 172.28.2.1:44500 |       |
--------------------------------------------------------------
| 4            | 172.28.2.2:44500 | 172.28.2.2:44500 |       |
--------------------------------------------------------------
| 5            | 172.28.2.3:44500 | 172.28.2.3:44500 |       |
--------------------------------------------------------------
| 6            | 172.28.2.1:44500 | 172.28.2.1:44500 |       |
--------------------------------------------------------------
| 7            | 172.28.2.2:44500 | 172.28.2.2:44500 |       |
--------------------------------------------------------------
| 8            | 172.28.2.3:44500 | 172.28.2.3:44500 |       |
--------------------------------------------------------------
| 9            | 172.28.2.1:44500 | 172.28.2.1:44500 |       |
--------------------------------------------------------------
| 10           | 172.28.2.2:44500 | 172.28.2.2:44500 |       |
--------------------------------------------------------------

nebula> SHOW TAGS;
===============
| ID | Name   |
===============
| 2 | player |
---------------
| 3 | team   |
---------------

nebula> SHOW CREATE TAG team;
===========================================================================
| Tag  | Create Tag                                                       |
===========================================================================
| team | CREATE TAG team (
  name string
) ttl_duration = 0, ttl_col = "" |
---------------------------------------------------------------------------
```

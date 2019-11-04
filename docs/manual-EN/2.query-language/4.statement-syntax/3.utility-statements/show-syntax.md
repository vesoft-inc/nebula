# Show Syntax

```
SHOW {SPACES | TAGS | EDGES | HOSTS}
SHOW CONFIGS [graph|meta|storage]
```

`SHOW SPACES` lists the SPACES on the Nebula Graph cluster.

`SHOW TAGS` and `SHOW EDGES` return the defined tags and edge types in the space, respectively.

`SHOW HOSTS` is to list storage hosts registered by the meta server. There are 6 columns: ip, port, status (online/offline), leader partitions count in all spaces, leader partitions count in each space, total partitions count in all spaces.

For more information about `SHOW CONFIGS [graph|meta|storage]`, please refer to [configs syntax](../../../3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/configs-syntax.md).

## Example

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

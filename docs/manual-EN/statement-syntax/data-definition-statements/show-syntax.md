```
SHOW {SPACES | TAGS | EDGES | HOSTS}
SHOW VARIABLES [graph|meta|storage] 
```

`SHOW SPACES` lists the SPACES on the Nebula Graph cluster. 

`SHOW TAGS` and `SHOW EDGES` return the defined tags and edge types in the space, respectively. 

`SHOW HOSTS` is to list storage hosts registered by the meta server.

For more information about `SHOW VARIABLES [graph|meta|storage]`, please refer to [variable syntax](../data-administration-statements/configuration-statements/variables-syntax.md).

### Example

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
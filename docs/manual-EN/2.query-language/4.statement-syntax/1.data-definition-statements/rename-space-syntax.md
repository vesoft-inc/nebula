# Rename Space Syntax

```ngql
RENAME SPACE <space_name> TO <new_space_name>
```

`RENAME SPACE` renames one graph space.  You must have `DROP` privileges for the original space, and `CREATE` and `INSERT` privileges for the new space.

For example, to rename a space named _old_space_ to _new_space_, use this statement:

```ngql
nebula> RENAME SPACE old_graph TO new_graph
```

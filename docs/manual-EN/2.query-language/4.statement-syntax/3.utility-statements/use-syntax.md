# Use Syntax

```
USE <graph_space_name>
```

The `USE` statement tells Nebula to use the named (graph) space as the current working space. This statement requires some privileges.

The named space remains the default until the end of the session or another `USE` statement is issued:

```SQL
nebula> USE space1
-- Traverse in graph space1
nebula> GO FROM 1 OVER edge1
nebula> USE space2
-- Traverse in graph space2. These nodes and edges have no relevance with space1.
nebula> GO FROM 1 OVER edge1
-- Now you are back to space1. Hereafter, you can not read any data from space2.
nebula> USE space1;
```

Different from SQL, making a space as the working space prevents you to access the other space. The only way to traverse in a new graph space is to switch by the `USE` statement.

> SPACES are `FULLY ISOLATED` from each other. Unlike SQL, which allows you to select two tables from different databases in one statement, in Nebula, you can only touch one space at a time.

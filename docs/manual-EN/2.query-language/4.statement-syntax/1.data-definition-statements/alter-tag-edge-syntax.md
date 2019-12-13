# Alter Tag/Edge Syntax

```ngql
ALTER TAG | EDGE <tag_name> | <edge_name>
    <alter_definition> [, alter_definition] ...]
    [ttl_definition [, ttl_definition] ... ]
  
alter_definition:
| ADD    (prop_name data_type)
| DROP   (prop_name)
| CHANGE (prop_name data_type)

ttl_definition:
    TTL_DURATION = ttl_duration, TTL_COL = prop_name
```

`ALTER` statement changes the structure of a tag or edge. For example, you can add or delete properties, change the data type of an existing property. You can also set a property as TTL (Time-To-Live), or change the TTL duration.

Multiple `ADD`, `DROP`, `CHANGE` clauses are permitted in a single `ALTER` statements, separated by commas. But do NOT add, drop, change the same property in one statement. If you have to do so, make each operation as a clause of the `ALTER` statement.

```ngql
ALTER TAG t1 ADD (id int, name string)

ALTER EDGE e1 ADD (prop1 int, prop2 string),    /* add prop1 */
              CHANGE (prop3 string),            /* change prop3 to string */
              DROP (prop4, prop5)               /* remove prop4 and prop5 */

ALTER EDGE e1 TTL_DURATION = 2, TTL_COL = prop1
```

Notice that TTL_COL only support INT and TIMESTAMP types.

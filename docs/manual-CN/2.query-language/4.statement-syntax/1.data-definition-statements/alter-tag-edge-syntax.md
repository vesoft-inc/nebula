# 修改 Tag / Edge

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

`ALTER` 语句可改变标签或边的结构，例如，可以添加或删除属性，更改已有属性的类型，也可将属性设置为 TTL（生存时间），或更改 TTL 时间。

一个 `ALTER` 语句允许使用多个 `ADD`，`DROP`，`CHANGE` 语句，语句之间需用逗号隔开。但是不要在一个语句中添加，删除或更改相同的属性。如果必须进行此操作，请将其作为 `ALTER` 语句的子语句。

```ngql
nebula> ALTER TAG t1 ADD (id int, name string)

nebula> ALTER EDGE e1 ADD (prop1 int, prop2 string),    /* 添加 prop1 */
              CHANGE (prop3 string),            /* 将 prop3 类型更改为字符 */
              DROP (prop4, prop5)               /* 删除 prop4 和 prop5 */

nebula> ALTER EDGE e1 TTL_DURATION = 2, TTL_COL = prop1  
```

注意 TTL_COL 仅支持 INT 和 TIMESTAMP 类型。

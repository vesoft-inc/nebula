# 修改Tag/Edge

```
ALTER {TAG | EDGE} tag_name | edge_name
    [alter_definition [, alter_definition] ...]
    [ttl_definition [, ttl_definition] ... ]

alter_definition:
| ADD    (prop_name data_type)
| DROP   (prop_name)
| CHANGE (prop_name data_type)

ttl_definition:
    TTL_DURATION = ttl_duration, TTL_COL = prop_name
```

> `ALTER` 可产生不同的结果，如果使用不匹配的版本/schema，其成行为是未定义的。

`ALTER` 语句可改变标签或边的结构，例如，可以添加或删除属性，更改已有属性的类型或重命名属性，也可
将属性设置为TTL（生存时间），或更改TTL时间。

一个`ALTER`语句允许使用多个`ADD`，`DROP`，`CHANGE`语句，语句之间需用逗号隔开。但是不要在一个
语句中添加，删除或更改相同的属性。如果必须进行此操作，请将其作为每个`ALTER`的子语句。

```
nebula> ALTER TAG t1 ADD (id int, name string)

nebula> ALTER EDGE e1 ADD (prop1 int, prop2 string),    /* 添加prop1 */
              CHANGE (prop3 string),            /* 将prop3类型更改为字符 */
              DROP (prop4, prop5)               /* 删除prop4和prop5 */

nebula> ALTER EDGE e1 TTL_DURATION = 2, TTL_COL = prop1  -- 注意prop1的值将在2秒内移除
```

注意TTL_COL仅支持INT和TIMESTAMP。

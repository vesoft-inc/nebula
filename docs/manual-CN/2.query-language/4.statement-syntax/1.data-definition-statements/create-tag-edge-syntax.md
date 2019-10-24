# CREATE TAG / EDGE 语法

```sql
CREATE {TAG | EDGE} tag_name|edge_name
    (create_definition, ...)
    [tag_edge_options]
  
create_definition:
    prop_name data_type
    
tag_edge_options:
    option [, option ...]

option:
    TTL_DURATION [=] ttl_duration
    | TTL_COL [=] prop_name
```

Nebula 的图结构由带有属性的 tags 和 edges 组成。`CREATE TAG` 使用一个给定的名称创建一个新的 tag。`CREATE EDGE` 则创建一个新的 edge type。

`CREATE TAG/EDGE` 语法有一些特点，在如下分块中将对这些特点进行讨论：

* [Tag 和 Edge Type 名称](#tag-name-and-edgetype-name)

* [属性名和数据类型](#property-name-and-data-type)

## Tag 名称和 Edge Type 名称

* **tag_name 和 edge_name**

    tags 和 edgeTypes 的名称在图中必须 **唯一**，且名称被定义后无法被修改。Tag 和 edgeType 的命名规则和 space 的命名规则一致。参见 [Schema Object Name](../../3.language-structure/schema-object-names.md)。

### 属性名和数据类

* **prop_name**

    prop_name 表示每个属性的名称。在每个 tag 和 edgeType 中必须唯一。

* **data_type**

    data_type 表示每个属性的数据类。更多关于 Nebula 支持的数据类型信息请参见 data-type 区文档。
    
    > NULL 和 NOT NULL 在创建 tag 和 edge 时不可用。(相比于关系型数据库).

### Time-to-Live (TTL) 语法

* TTL_DURATION

    TTL_DURATION 指定了 vertices 和 edges 的有效期，超过有效期的数据会失效。失效时间为 TTL_COL 设置的属性值加 TTL_DURATION 设置的秒数。

    > 如果 TTL_DURATION 的值为负或0，则该 edge 不会失效。

* TTL_COL

    指定的列（或者属性）必须是 int64 或者 timestamp.

* 多 TTL 定义

    可以指定多个 TTL_COL 字段， **Nebula Graph** 会使用最早的失效时间。

### 示例

```
CREATE TAG course(name string, credits int) 
CREATE TAG notag()  -- empty properties

CREATE EDGE follow(start_time timestamp, likeness double)
CREATE EDGE noedge()  -- empty properties

CREATE TAG woman(name string, age int, 
   married bool, salary double, create_time timestamp)
   TTL_DURATION = 100, TTL_COL = create_time -- expired when now is later than create_time + 100
   
CREATE EDGE marriage(location string, since timestamp)
    TTL_DURATION = 0, TTL_COL = since -- negative or zero, not expire
   
CREATE TAG icecream(made timestamp, temperature int)
   TTL_DURATION = 100, TTL_COL = made,
   TTL_DURATION = 10, TTL_COL = temperature 
   --  no matter which comes first: made + 100 or temperature + 10
 
CREATE EDGE garbage (thrown timestamp, temperature int)
   TTL_DURATION = -2, TTL_COL = thrown, 
   TTL_DURATION = 10, TTL_COL = thrown 
   --  legal, but not recommended. expired at thrown + 10
```


# CREATE TAG / EDGE 语法

```ngql
CREATE {TAG <tag_name> | EDGE <edge_name>} [IF NOT EXISTS]
    ([<create_definition>, ...])
    [tag_edge_options]
  
<create_definition> ::=
    <prop_name> <data_type>

<tag_edge_options> ::=
    <option> [, <option> ...]

<option> ::=
    TTL_DURATION [=] <ttl_duration>
    | TTL_COL [=] <prop_name>
    | DEFAULT <default_value>
```

**Nebula Graph** 的图结构由带有属性的 tags 和 edges 组成。`CREATE TAG` 使用一个给定的名称创建一个新的 tag。`CREATE EDGE` 则创建一个新的 edge type。

`CREATE TAG/EDGE` 语法有一些特点，在如下分块中将对这些特点进行讨论：

## IF NOT EXISTS

创建 tag 或 edge 可使用 `IF NOT EXISTS` 关键字，这个关键字会自动检测对应的 tag 或 edge 是否存在，如果不存在则创建新的，如果存在则直接返回。

**注意：** 这里判断 tag 或 edge 是否存在只是比较 tag 或 edge 的名字(不包括属性)。

## Tag 名称和 Edge Type 名称

* **tag_name 和 edge_name**

    tags 和 edgeTypes 的名称在图中必须 **唯一**，且名称被定义后无法被修改。Tag 和 edgeType 的命名规则和 space 的命名规则一致。参见 [Schema Object Name](../../3.language-structure/schema-object-names.md)。

### 属性名和数据类型

* **prop_name**

    prop_name 表示每个属性的名称。在每个 tag 和 edgeType 中必须唯一。

* **data_type**

    data_type 表示每个属性的数据类型。更多关于 **Nebula Graph** 支持的数据类型信息请参见 [data-type](../../1.data-types/data-types.md)。

    > NULL 和 NOT NULL 在创建 tag 和 edge 时不可用。(相比于关系型数据库)。

* **默认值约束**
  
  您可以在创建标签/边时使用 `DEFAULT` 约束设置属性的默认值。如果没有指定其他值，那么会将默认值插入新的顶点或边。如果您不想使用默认值，也可以写一个用户指定的值。
  
  > 暂时不支持使用 `Alter` 更改默认值。

<!-- ### Time-to-Live (TTL) 语法

* TTL_DURATION

    TTL_DURATION 指定了 vertices 和 edges 的有效期，超过有效期的数据会失效。失效时间为 TTL_COL 设置的属性值加 TTL_DURATION 设置的秒数。

    > 如果 TTL_DURATION 的值为负或 0，则该 edge 不会失效。

* TTL_COL

    指定的列（或者属性）必须是 int64 或者 timestamp。

* 多 TTL 定义

    可以指定多个 TTL_COL 字段， **Nebula Graph** 会使用最早的失效时间。 -->

### 示例

```ngql
nebula> CREATE TAG course(name string, credits int)
nebula> CREATE TAG notag()  -- 属性为空

nebula> CREATE EDGE follow(start_time timestamp, grade double)
nebula> CREATE EDGE noedge()  -- 属性为空

nebula> CREATE TAG player_with_default(name string, age int DEFAULT 20)  -- 默认年龄设置为 20 岁
nebula> CREATE EDGE follow_with_default(start_time timestamp DEFAULT 0, grade double DEFAULT 0.0)  -- 默认 start_time 设置为 0，默认 grade 设置为 0.0
```

<!-- ```ngql
nebula> CREATE TAG woman(name string, age int,
   married bool, salary double, create_time timestamp)
   TTL_DURATION = 100, TTL_COL = create_time -- 过期时间是 100， 从当前时间开始

nebula> CREATE EDGE marriage(location string, since timestamp)
    TTL_DURATION = 0, TTL_COL = since -- 负值或 0 数据不会失效

nebula> CREATE TAG icecream(made timestamp, temperature int)
   TTL_DURATION = 100, TTL_COL = made,
   TTL_DURATION = 10, TTL_COL = temperature
   --  超过任一 TTL_DURATION 数据即失效

nebula> CREATE EDGE garbage (thrown timestamp, temperature int)
   TTL_DURATION = -2, TTL_COL = thrown,
   TTL_DURATION = 10, TTL_COL = thrown
   --  语法合法，但不推荐。数据将在 thrown + 10 后失效
``` -->

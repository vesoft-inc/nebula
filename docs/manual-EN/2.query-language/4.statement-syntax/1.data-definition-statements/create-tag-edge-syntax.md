# Create TAG / EDGE Syntax

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

**Nebula Graph**'s schema is composed of tags and edges, either of which may have properties. `CREATE TAG` statement defines a tag with the given name. While `CREATE EDGE` statement is to define an edge type.

The features of this syntax are described in the following sections:

## IF NOT EXISTS

You can use the `If NOT EXISTS` keywords when creating tags or edges. This keyword automatically detects if the corresponding tag or edge exists. If it does not exist, a new one is created. Otherwise, no tag or edge is created.

**Note:** The tag or edge existence detection here only compares the tag or edge name (excluding properties).

## Tag Name and Edge Type Name

* **tag_name and edge_name**

    The name of tags and edgeTypes must be **unique** within the space. Once the name is defined, it can not be altered. The rules of tag and edgeType names are the same as those for names of spaces. See [Schema Object Name](../../3.language-structure/schema-object-names.md) for detail.

### Property Name and Data Type

* **prop_name**

    prop_name indicates the name of properties. It must be unique for each tag or edgeType.

* **data_type**

    data_type represents the data type of each property. For more information about data types that **Nebula Graph** supports, see [data-type](../../1.data-types/data-types.md) section.

    > NULL and NOT NULL constrain are not supported yet when creating tags/edges (comparing with relational databases).

* **Default Constraint**

    You can set the default value of a property when creating a tag/edge with the `DEFAULT` constraint. The default value will be added to all new vertices and edges IF no other value is specified. Also you can write a user-specified value if you don't want to use the default one.

    > Using `Alter` to change the default value is not supported.

    <!-- > Since it's so error-prone to modify the default value with new one, using `Alter` to change the default value is not supported. -->

<!-- ### Time-to-Live (TTL) Syntax

* TTL_DURATION

    ttl_duration specifies the life cycle of vertices (or edges). Data that exceeds the specified TTL will expire. The expiration threshold is the specified TTL_COL value plus the TTL_DURATION.

    > If the value for ttl_duration is zero or negative, the vertices or edges will not expire.

* TTL_COL

    The data type of prop_name must be either int64 or timestamp.

* multiple TTL definition

    If TTL_COL is a list of prop_name, and there are multiple ttl_duration, **Nebula Graph** uses the lowest(i.e. earliest) expiration threshold to expire data. -->

### Examples

```ngql
nebula> CREATE TAG course(name string, credits int)
nebula> CREATE TAG notag()  -- empty properties

nebula> CREATE EDGE follow(start_time timestamp, grade double)
nebula> CREATE EDGE noedge()  -- empty properties

nebula> CREATE TAG player_with_default(name string, age int DEFAULT 20)  -- age is set to 20 by default
nebula> CREATE EDGE follow_with_default(start_time timestamp DEFAULT 0, grade double DEFAULT 0.0)  -- start_time is set to 0 by default, grade is set to 0.0 by default
```

<!-- ```ngql
nebula> CREATE TAG woman(name string, age int,
   married bool, salary double, create_time timestamp)
   TTL_DURATION = 100, TTL_COL = create_time -- expired when now is later than create_time + 100

nebula> CREATE EDGE marriage(location string, since timestamp)
    TTL_DURATION = 0, TTL_COL = since -- negative or zero, not expire

nebula> CREATE TAG icecream(made timestamp, temprature int)
   TTL_DURATION = 100, TTL_COL = made,
   TTL_DURATION = 10, TTL_COL = temperature
   --  no matter which comes first: made + 100 or temprature + 10

nebula> CREATE EDGE garbge (thrown timestamp, temprature int)
   TTL_DURATION = -2, TTL_COL = thrown,
   TTL_DURATION = 10, TTL_COL = thrown
   --  legal, but not recommended. expired at thrown + 10
``` -->

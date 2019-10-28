# Create TAG / EDGE Syntax

```
CREATE {TAG | EDGE} <tag_name> | <edge_name>
    (<create_definition>, ...)
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

Nebula's graph schema is composed of tags and edges, either of which may have properties. `CREATE TAG` statement defines a tag with the given name. While `CREATE EDGE` statement is to define an edge type.

There are several aspects to this syntax, described under the following topics in this section:

<!---
* [Tag Name and Edge Type Name](#tag-name-and-edgetype-name)

* [Property Name and Data Type](#property-name-and-data-type)
-->

## Tag Name and Edge Type Name

* **tag_name and edge_name**

    The name of tags and edgeTypes should be **unique** within the space. Once the name is defined, it can not be altered. The rules of tag and edgeType names are the same as those for names of spaces. See [Schema Object Name](../../3.language-structure/schema-object-names.md)

### Property Name and Data Type

* **prop_name**

    prop_name indicates the name of properties. It must be unique for each tag or edgeType.

* **data_type**

    data_type represents the data type of each property. For more information about data types that Nebula Graph supports, see data types section.
    
    > NULL and NOT NULL constrain are not supported yet when creating tags/edges (comparing with relational databases).

* **default values**

    You can set the default value of a property when creating a tag/edge. When inserting a new vertex or edge, you don't have to provide the value for that property. Also you can write a user-specify value if you don't want to use the default one.

    > Since it's so error-prone to modify the default value with new one, using `Alter` to change the default value is not supported.

### Time-to-Live (TTL) syntax

* TTL_DURATION

    ttl_duration specifies the life cycle of vertices (or edges). TTL expires the vertexes or edges after the specified number of seconds has passed since the TTL_COL configured value; i.e. the expiration threshold is TTL_COL configured property's value plus the specified number of seconds.

    > If the value for ttl_duration is zero or negative, the vertices or edges will not expire.

* TTL_COL

    The data type of prop_name must be either int64 or timestamp.

* multiple TTL definition

    If TTL_COL is a list of prop_name, and there are multiple ttl_duration, **Nebula Graph** uses the lowest(i.e. earliest) expiration threshold to expire data.

### Examples

```
CREATE TAG course(name string, credits int) 
CREATE TAG notag()  -- empty properties

CREATE EDGE follow(start_time timestamp, likeness double)
CREATE EDGE noedge()  -- empty properties

CREATE TAG course_with_default(name string, credits int DEFAULT 0)  -- credits is set 0 by default
CREATE EDGE follow_with_default(start_time timestamp DEFAULT 0, likeness double 0.0)

CREATE TAG woman(name string, age int, 
   married bool, salary double, create_time timestamp)
   TTL_DURATION = 100, TTL_COL = create_time -- expired when now is later than create_time + 100
   
CREATE EDGE marriage(location string, since timestamp)
    TTL_DURATION = 0, TTL_COL = since -- negative or zero, not expire
   
CREATE TAG icecream(made timestamp, temprature int)
   TTL_DURATION = 100, TTL_COL = made,
   TTL_DURATION = 10, TTL_COL = temprature 
   --  no matter which comes first: made + 100 or temprature + 10
 
CREATE EDGE garbge (thrown timestamp, temprature int)
   TTL_DURATION = -2, TTL_COL = thrown, 
   TTL_DURATION = 10, TTL_COL = thrown 
   --  legal, but not recommended. expired at thrown + 10
```


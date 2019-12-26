# Insert Edge Syntax

```ngql
INSERT EDGE <edge_name> ( <prop_name_list> ) VALUES | VALUE
<src_vid> -> <dst_vid>[@<ranking>] : ( <prop_value_list> )
[, <src_vid> -> <dst_vid> : ( <prop_value_list> ), ...]

<prop_name_list> ::=
  [ <prop_name> [, <prop_name> ] ...]

<prop_value_list> ::=
  [ <prop_value> [, <prop_value> ] ...]
```

`INSERT EDGE` statement inserts a (directed) edge from a starting vertex (given by src_vid) to an ending vertex (given by dst_vid).

* `<edge_name>` denotes the edge type, which must be created before `INSERT EDGE`.
* `<prop_name_list>` is the property name list as the given `<edge_name>`.
* `<prop_value_list>` must provide the value list according to `<prop_name_list>`. If no value matches the type, an error will be returned.
* `ranking` is optional, it specifies the edge ranking of the same edge type, if not specified, the default value is 0.

> No default value is given in this release.

## Examples

```ngql
nebula> CREATE EDGE e1()                    -- create edge t1 with empty property or default values
nebula> INSERT EDGE e1 () VALUES 10->11:()    -- insert an edge from vertex 10 to vertex 11 with empty property
nebula> INSERT EDGE e1 () VALUES 10->11@1:()  -- insert an edge from vertex 10 to vertex 11 with empty property, the edge ranking is 1
```

```ngql
nebula> CREATE EDGE e2 (name string, age int)                     -- create edge e2 with two properties
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", 1)          -- insert edge from 11 to 13 with two properties
nebula> INSERT EDGE e2 (name, age) VALUES \
12->13:("n1", 1), 13->14("n2", 2)                           -- insert two edges
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", "a13")      -- ERROR. "a13" is not int
```

An edge can be inserted/wrote multiple times. Only the last written values can be read.

```ngql
-- insert edge with the new values.
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", 12)
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", 13)
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", 14) -- the last version can be read
```

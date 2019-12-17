# Insert Vertex Syntax

```ngql
INSERT VERTEX <tag_name> [, <tag_name>, ...] (prop_name_list[, prop_name_list])
     {VALUES | VALUE} vid: (prop_value_list[, prop_value_list])

prop_name_list:
  [prop_name [, prop_name] ...]

prop_value_list:
  [prop_value [, prop_value] ...]
```

INSERT VERTEX statement inserts one vertex into **Nebula Graph**.

* `tag_name` denotes the `tag` (vertex type), which must be created before `INSERT VERTEX`.
* `prop_name_list` is the property name list in the given `tag_name`.
* `prop_value_list` must provide the value list according to the `prop_name_list`. If no value matches the type, an error will be returned.

> No default value is given in this release.

## Examples

```ngql
nebula> CREATE TAG t1()                   -- create tag t1 with empty property
nebula> INSERT VERTEX t1 () VALUES 10:()    -- insert vertex 10 with no property
```

```ngql
nebula> CREATE TAG t2 (name string, age int)                -- create tag t2 with two properties
nebula> INSERT VERTEX t2 (name, age) VALUES 11:("n1", 12)     -- insert vertex 11 with two properties
nebula> INSERT VERTEX t2 (name, age) VALUES 12:("n1", "a13")  -- ERROR. "a13" is not int
nebula> INSERT VERTEX t2 (name, age) VALUES 13:("n3", 12), 14:("n4", 8)    -- insert two vertices
```

```ngql
nebula> CREATE TAG t1(i1 int)
nebula> CREATE TAG t2(s2 string)
nebula> INSERT VERTEX  t1 (i1), t2(s2) VALUES 21: (321, "hello")   -- insert vertex 21 with two tags.
```

A vertex can be inserted/wrote multiple times. Only the last written values can be read.

```ngql
-- insert vertex 11 with the new values.
nebula> INSERT VERTEX t2 (name, age) VALUES 11:("n2", 13)
nebula> INSERT VERTEX t2 (name, age) VALUES 11:("n3", 14)
nebula> INSERT VERTEX t2 (name, age) VALUES 11:("n4", 15)  -- the last version can be read
```

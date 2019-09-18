```
INSERT VERTEX tag_name[, tag_name] (prop_name_list[, prop_name_list])
     {VALUES | VALUE} vid: (prop_value_list[, prop_value_list])

prop_name_list:
  [prop_name [, prop_name] ...]

prop_value_list:
  [prop_value [, prop_value] ...]
```

INSERT VERTEX statements inserts one vertex into Nebula.
* `tag_name` denotes the `tag` (vertex type), which must be created before `INSERT VERTEX`.
* `prop_name_list` is the property name list as the given `tag_name`.
* `prop_value_list` must provide the value list according to `prop_name_list`. If any value doesn't match its type, an error will be returned.

> No default value is given in this release.

### Examples

```
# CREATE TAG t1()                   -- create tag t1 with empty property
INSERT VERTEX t1 () VALUES 10:()    -- insert a vertex with vid 100 and empty property
```

```
# CREATE TAG t2 (name string, age int)                -- create tag t2 with two properties
INSERT VERTEX t2 (name, age) VALUES 11:("n1", 12)     -- insert vertex 11 with the properties
INSERT VERTEX t2 (name, age) VALUES 12:("n1", "a13")  -- WRONG. "a13" is not int
```

```
# CREATE TAG t1(i1 int)
# CREATE TAG t2(s2 string)
INSERT VERTEX  t1 (i1), t2(s2) VALUES 21: (321, "hello")   -- insert vertex 21 with two tags.
```

A vertex can be inserted/wrote multiple times. Only the last write values can be read.

```
-- insert vertex 11 with the new version of values.
INSERT VERTEX t2 (name, age) VALUES 11:("n2", 13)
INSERT VERTEX t2 (name, age) VALUES 11:("n3", 14)
INSERT VERTEX t2 (name, age) VALUES 11:("n4", 15)  -- the last version can be read
```

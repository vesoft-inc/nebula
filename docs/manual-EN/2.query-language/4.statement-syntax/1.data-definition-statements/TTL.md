# TTL (time-to-live)

With **TTL**, **Nebula Graph** provides the ability to delete the expired vertices or edges automatically. The system will automatically delete the expired data during the compaction phase. Before compaction, query will filter the expired data.

TTl requires `ttl_col` and `ttl_duration` together. `ttl_col` indicates the TTL column, while `ttl_duration` indicates the duration of the TTL. When the sum of the TTL column and the ttl_duration is less than the current time, we consider the data as expired. The `ttl_col` type is integer or timestamp, and is set in seconds. `ttl_duration` is also set in seconds.

## TTL configuration

- The `ttl_duration` is set in seconds. If it is set to -1 or 0, the vertex properties of this tag does not expire.

- If TTL is set, when the sum of the `ttl_col` and the `ttl_duration` is less than the current time, we consider the vertex properties of this tag as expired after the specified seconds configured by `ttl_duration` has passed since the `ttl_col` field value.

- When the vertex has multiple tags, the TTL of each tag is processed separately.

## Setting a TTL Value

- Setting a TTL value for the existed tag.

```ngql
nebula> CREATE TAG t1(a timestamp);
nebula> ALTER TAG t1 ttl_col = "a", ttl_duration = 5; -- Setting ttl
nebula> INSERT VERTEX t1(a) values 101:(now());
```

The vertex 101 property in tag t1 will expire after 5 seconds since specified by now().

- Or you can set the TTL attribute when creating the tag.

```ngql
nebula> CREATE TAG t2(a int, b int, c string) ttl_duration= 100, ttl_col = "a";
nebula> INSERT VERTEX t2(a, b, c) values 102:(1584441231, 30, "Word");
```

The vertex 102 property in tag t2 will expire after 100 seconds since March 17 2020 at 18:33:51 CST i.e. the timestamp is 1584441231.

- When a vertex has multiple TAGs, the TTL of each TAG is independent from each other.

```ngql
nebula> CREATE TAG t3(a string);
nebula> INSERT VERTEX t1(a),t3(a) values 200:(now(), "hello");
```

The vertex 200 property in tag t1 will expire after 5 seconds.

```ngql
nebula> FETCH PROP ON t1 200;
Execution succeeded (Time spent: 5.945/7.492 ms)

nebula> FETCH PROP ON t3 200;
======================
| VertexID | t3.a    |
======================
| 200      | hello   |
----------------------

nebula> FETCH PROP ON * 200;
======================
| VertexID | t3.a    |
======================
| 200      | hello   |
----------------------
```

## Dropping TTL

If you have set a TTL value for a field and later decide do not want it to ever automatically expire, you can drop the TTL value, set it to an empty string or invalidate it by setting it to 0 or -1.

```ngql
nebula> ALTER TAG t1 ttl_col = ""; -- drop ttl attribute;
```

Drop the field a with the ttl attribute:

```ngql
nebula> ALTER TAG t1 DROP (a); -- drop ttl_col
```

Invalidate the TTL:

```ngql
nebula> ALTER TAG t1 ttl_duration = 0; -- keep the ttl but the data never expires
```

## Tips on TTL

- If a field contains a `ttl_col` field, you can't make any change on the field.

``` ngql
nebula> CREATE TAG t1(a int, b int, c string) ttl_duration = 100, ttl_col = "a";
nebula> ALTER TAG t1 CHANGE (a string); -- failed
```

- Note that the a tag or an edge cannot have both the TTL attribute and index at the same time, even if the `ttl_col` column is different from that of the index.

``` ngql
nebula> CREATE TAG t1(a int, b int, c string) ttl_duration = 100, ttl_col = "a";
nebula> CREATE TAG INDEX id1 ON t1(a); -- failed
```

``` ngql
nebula> CREATE TAG t1(a int, b int, c string) ttl_duration = 100, ttl_col = "a";
nebula> CREATE TAG INDEX id1 ON t1(b); -- failed
```

```ngql
nebula> CREATE TAG t1(a int, b int, c string);
nebula> CREATE TAG INDEX id1 ON t1(a);
nebula> ALTER TAG t1 ttl_col = "a", ttl_duration = 100; -- failed
```

- Adding TTL to an edge is similar to a tag.

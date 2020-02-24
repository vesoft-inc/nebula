# TTL (time-to-live)

With **TTL**, **Nebula Graph** provides the ability to filter the expired data automatically when traversing vertices and edges. The system will automatically delete the expired data during the compaction phase, without a delete operation that is explicitly issued by the console.

`ttl_col` indicates the ttl column, while `ttl_duration` indicates the duration of the ttl. When the sum of the ttl column and the ttl_duration is less than the current time, we consider the data as expired. The `ttl_col` type is integer or timestamp, and is set in seconds. `ttl_duration` is also set in seconds.

## TTL configurations

The time to live value is set in seconds.

- If TTL is set, when the sum of the `ttl_col` and the `ttl_duration` is less than the current time, we consider the data as expired.

- If TTL is not set or the `ttl_col` is null, then the time to live has no effect.

- If TTL is set to -1 or 0, then data in the field does not expire.

## Setting a TTL Value

Setting a TTL value allows you to specify the living time of the data.

Creating a tag then adding ttl attribute.

```ngql
nebula> CREATE TAG t(a int, b int, c string);
nebula> ALTER TAG t ADD ttl_col = "a", ttl_duration = 1000; -- add ttl attribute
nebula> SHOW CREATE TAG t;
```

Or you can set the TTL attribute when creating the tag.

```ngql
nebula> CREATE TAG t(a int, b int, c string) ttl_duration= 100, ttl_col = "a";
```

## Dropping TTL

If you have set a TTL value for a field and later decide do not want it to ever automatically expire, you can drop the TTL value. For example, using the previous example, drop the TTL value on field `a`.

Drop the entire field directly:

```ngql
nebula> ALTER TAG t DROP a; -- drop field a with the ttl attribute
nebula> SHOW CREATE TAG t;
```

Or you can invalidate the TTL with the following method:

```ngql
nebula> ALTER TAG t ttl_col = ""; -- drop ttl attribute
nebula> ALTER TAG t ttl_duration = 0; -- keep the ttl but the data never expires
```

## Tips on TTL

- If a field contains a TTL value, you can't make any change on the field.

``` ngql
nebula> ALTER TAG t ADD ttl_col = "b", ttl_duration = 1000;
nebula> ALTER TAG t CHANGE (b string); -- failed
```

- Note that the a tag or an edge cannot have both the TTL attribute and index at the same time.

``` ngql
nebula> CREATE TAG t(a int, b int, c string) ttl_duration = 100, ttl_col = "a";
nebula> CREATE TAG INDEX id1 ON t(a); -- failed
```

```ngql
nebula> CREATE TAG t1(a int, b int, c string);
nebula> CREATE TAG INDEX id1 ON t1(a);
nebula> ALTER TAG t1 ADD ttl_col = "a", ttl_duration = 100; -- failed
```

- Adding TTL to an edge is similar to a tag.

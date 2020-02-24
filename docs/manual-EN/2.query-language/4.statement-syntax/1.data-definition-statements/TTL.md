# TTL (time-to-live)

With **TTL**, **Nebula Graph** provides the ability to delete data automatically from a graph space in the compaction phase after when the data expires. After you set the TTL , **Nebula Graph** will automatically delete these data after TTL is expired, since the time they were last modified. Time to live value is configured in seconds. When you configure TTL, the system will automatically delete the expired data based on the TTL value, without a delete operation that is explicitly issued by the console.

## TTL configurations

The time to live value is set in seconds.

- If TTL is set to `n`, then data in the field will expire after n seconds.

- If TTL is not set, then the time to live has no effect.

- If TTL is set to -1 or 0, then data in the filed does not expire.

## Setting a TTL Value

Setting a TTL value allows you to identify the number of seconds the data will live before it is expired.

```ngql
nebula> CREATE TAG t(a int, b int, c string);
nebula> ALTER TAG t ADD ttl_col = "a", ttl_duration = 1000; -- add ttl attribute
nebula> SHOW CREATE TAG t;
```

Or you can create a TTL when creating the tag.

```ngql
nebula> CREATE TAG t(a int, b int, c string) ttl_duration= 100, ttl_col = "a";
```

## Dropping TTL Expiration

If you have set a TTL value for a field and later decide do not want it to ever automatically expire, you can drop the TTL value. For example, using the previous example, drop the TTL value on field `a`.

Drop the entire field directly:

```ngql
nebula> ALTER TAG t DROP a; -- drop field a with the ttl attribute
nebula> SHOW CREATE TAG t;
```

Or you can invalidate the TTL with the following method:

```ngql
nebula> ALTER TAG t ADD ttl_col = ""; ---- drop ttl attribute
```

## Tips on TTL

- If a field contains a TTL value, you can't make any change on it.

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

# Configure TTL

With **Time to Live** or TTL, **Nebula Graph** provides the ability to delete data automatically from a graph space after a certain time period. After you set the TTL , **Nebula Graph** will automatically remove these data after the time period, since the time they were last modified. Time to live value is configured in seconds. When you configure TTL, the system will automatically delete the expired data based on the TTL value, without needing a delete operation that is explicitly issued by the console.

## TTL configurations

The time to live value is set in seconds, and it is interpreted as a delta from the time that data was last modified.

- If TTL is set to `n`, then data in the column will expire after n seconds.

- If TTL is not set, then the time to live has no effect.

- If TTL is set to -1, then data in the column does not expire.

## Setting a TTL Value

Setting a TTL value allows you to identify the number of seconds the data will live before expiring.

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

If you have set a TTL value for a column and later decide do not want it to ever automatically expire, you can drop the TTL value. For example, using the previous example, drop the TTL value on column `a`.

```ngql
nebula> ALTER TAG t DROP a; -- drop ttl attribute column
nebula> SHOW CREATE TAG t;
```

Or you can delete with the following method:

```ngql
nebula> ALTER TAG t ADD ttl_col = ""; ---- drop ttl attribute
```

## Tips on TTL

- If a column contains a TTL value, you can't make any change on it.

``` ngql
nebula> ALTER TAG t ADD ttl_col = "b", ttl_duration = 1000;
nebula> ALTER TAG t CHANGE (b string); -- failed
```

- Note that the a tag or an edge can only have either TTL attribute or index at the same time.

``` ngql
nebula> CREATE TAG t(a int, b int, c string) ttl_duration= 100, ttl_col = "a";
nebula> CREATE TAG INDEX id1 ON t(a); -- failed
```

```ngql
nebula> CREATE TAG t1(a int, b int, c string);
nebula> CREATE TAG INDEX id1 ON t1(a);
nebula> ALTER TAG t1 ADD ttl_col = "a", ttl_duration = 100; -- failed
```

- Adding TTL to an edge is similar to a tag.

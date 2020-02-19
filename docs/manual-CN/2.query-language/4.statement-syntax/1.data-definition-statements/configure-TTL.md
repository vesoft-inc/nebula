# 配置 TTL（生存时间）

借助**生存时间**或 TTL，**Nebula Graph** 支持在特定时间段后自动删除数据。在设置 TTL 后，**Nebula Graph** 会在一段时间（自上次修改数据的时间开始算起）后自动删除这些数据。 配置的生存时间值以秒为单位。 配置 TTL 后，系统会基于 TTL 值自动删除已过期的数据，而不需要客户端显式发出的 delete 操作。

## 生存时间配置

生存时间值是以秒为单位设置的，解释为自上次修改数据的时间的增量。

- 如果将 TTL 设置为 `n` ，则该 column 数据将在 n 秒后过期。

- 如果设置未 TTL，则生存时间不起作用。

- 如果 TTL 设置为 -1，则此 column 数据不会过期。

## 设置生存时间

设置 TTL 值可用来确定数据在过期之前存活的秒数。

```ngql
nebula> CREATE TAG t(a int, b int, c string);
nebula> ALTER TAG t ADD ttl_col = "a", ttl_duration = 1000; -- add ttl attribute
nebula> SHOW CREATE TAG t;
```

或亦可在创建 tag 时创建 TTL。

```ngql
nebula> CREATE TAG t(a int, b int, c string) ttl_duration= 100, ttl_col = "a";
```

## 删除生存时间

如果为 column 设置了 TTL 值，但又不希望数据自动过期，则可以删除该 TTL 值。例如，接着前面的示例，删除 column `a` 的 TTL 值。

```ngql
nebula> ALTER TAG t DROP a; -- drop ttl attribute column
nebula> SHOW CREATE TAG t;
```

或使用如下方式删除：

```ngql
nebula> ALTER TAG t ADD ttl_col = ""; ---- drop ttl attribute
```

## 生存时间使用注意事项

- 如果 column 包含 TTL，则不支持更改操作。

``` ngql
nebula> ALTER TAG t ADD ttl_col = "b", ttl_duration = 1000;
nebula> ALTER TAG t CHANGE (b string); -- failed
```

- 注意一个 tag 或 edge 不能同时拥有 TTL 和索引，只能二者择其一。

``` ngql
nebula> CREATE TAG t(a int, b int, c string) ttl_duration= 100, ttl_col = "a";
nebula> CREATE TAG INDEX id1 ON t(a); -- failed
```

```ngql
nebula> CREATE TAG t1(a int, b int, c string);
nebula> CREATE TAG INDEX id1 ON t1(a);
nebula> ALTER TAG t1 ADD ttl_col = "a", ttl_duration = 100; -- failed
```

- edge 添加 TTL 与 tag 类似。

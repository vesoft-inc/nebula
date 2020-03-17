# TTL (time-to-live)

Nebula 支持 **TTL** ，在一定时间后或在特定时钟时间自动从数据库中删除点或者边。过期数据会在下次 compaction 时被删除，在下次 compaction 来临前，query 会过滤掉过期的点和边。

ttl 功能需要 `ttl_col` 和 `ttl_duration` 一起使用。自从 `ttl_col` 指定的字段的值起，经过 `ttl_duration` 指定的秒数后，该条数据过期。即，到期阈值是 ttl_col 指定的 property 的值加上 `ttl_duration` 设置的秒数。其中 `ttl_col` 指定的字段的类型需为 integer 或者 timestamp。

## TTL 配置

- `ttl_duration` 单位为秒，当 `ttl_duration` 被设置为 -1 或者 0，则此条数据不会过期。

- 当 `ttl_col` 指定的字段的值加上 `ttl_duration` 值小于当前时间时，该条数据过期。

- 当该条数据有多个 tag，每个 tag 的 ttl 单独处理。


## 设置 TTL

* 对已经创建的 tag，设置 TTL。

```ngql
nebula> CREATE TAG t(a timestamp, b int, c string);
nebula> ALTER TAG t ttl_col = "a", ttl_duration = 100; -- 创建 ttl
nebula> SHOW CREATE TAG t;
```
此 tag 的点会在 `a` 字段的值，经过 100s 后过期。

* 在创建 tag 时设置 TTL。

```ngql
nebula> CREATE TAG t(a timestamp, b int, c string) ttl_duration= 100, ttl_col = "a";
```

```ngql
nebula> CREATE TAG t1(a timestamp) ttl_duration= 5, ttl_col = "a";
nebula> CREATE TAG t2(a timestamp) ttl_duration= 10, ttl_col = "a"
nebula> INSERT VERTEX t1(a),t2(a) values 100:(now(), now()+15)
```
从 now() 起，5s 后，t1 过期，20s 后，t2 过期。

当点有多个 tag 时

```ngql
nebula> CREATE TAG t3(a string)
nebula> INSERT VERTEX t1(a),t3(a) values 200:(now(), "hello")
```

5s 后, 点 Vertex 200 的 t1 属性过期。

```ngql
nebula> fetch prop on t1 200
Execution succeeded (Time spent: 5.945/7.492 ms)


nebula> fetch prop on t3 200
======================
| VertexID | t3.name |
======================
| 200      | hello   |
----------------------
```


## 删除 TTL

如果想要删除 TTL，可以 设置 `ttl_col` 字段为空，或删除配置的 `ttl_col` 字段，或者设置 `ttl_duration` 为 0 或者 -1。


```ngql
nebula> ALTER TAG t ttl_col = ""; -- drop ttl attribute
```

删除配置的 ttl_col 字段：

```ngql
nebula> ALTER TAG t DROP a; -- drop field a with the ttl attribute
```

设置 ttl_duration 为 0 或者 -1：

```ngql
nebula> ALTER TAG t ttl_duration = 0; -- keep the ttl but the data never expires
```

## TTL 使用注意事项

- 不能修改 `ttl_col` 所配置的字段。

``` ngql
nebula> ALTER TAG t ADD ttl_col = "b", ttl_duration = 1000;
nebula> ALTER TAG t CHANGE (b string); -- failed
```

- 注意一个 tag 或 edge 不能同时拥有 TTL 和索引，只能二者择其一，即使 `ttl_col` 配置的字段与要创建索引的字段不同。

``` ngql
nebula> CREATE TAG t(a int, b int, c string) ttl_duration = 100, ttl_col = "a";
nebula> CREATE TAG INDEX id1 ON t(a); -- failed
```

``` ngql
nebula> CREATE TAG t(a int, b int, c string) ttl_duration = 100, ttl_col = "a";
nebula> CREATE TAG INDEX id1 ON t(b); -- failed
```

```ngql
nebula> CREATE TAG t1(a int, b int, c string);
nebula> CREATE TAG INDEX id1 ON t1(a);
nebula> ALTER TAG t1 ADD ttl_col = "a", ttl_duration = 100; -- failed
```

- 对 edge 配置 TTL 与 tag 类似。

# TTL (time-to-live)

**Nebula Graph** 支持 **TTL** ，在一定时间后自动从数据库中删除点或者边。过期数据会在下次 compaction 时被删除，在下次 compaction 前，query 会过滤掉过期的点和边。

ttl 功能需要 `ttl_col` 和 `ttl_duration` 一起使用。自从 `ttl_col` 指定的字段的值起，经过 `ttl_duration` 指定的秒数后，该条数据过期。即，到期阈值是 `ttl_col` 指定的 property 的值加上 `ttl_duration` 设置的秒数。其中 `ttl_col` 指定的字段的类型需为 integer 或者 timestamp。

## TTL 配置

- `ttl_duration` 单位为秒，当 `ttl_duration` 被设置为 -1 或者 0，则点的此 tag 属性不会过期。

- 当 `ttl_col` 指定的字段的值 + `ttl_duration` 值 < 当前时间时，该条数据此 tag 属性值过期。

- 当该条数据有多个 tag，每个 tag 的 ttl 单独处理。

## 设置 TTL

- 对已经创建的 tag，设置 TTL。

```ngql
nebula> CREATE TAG t1(a timestamp);
nebula> ALTER TAG t1 ttl_col = "a", ttl_duration = 5; -- 创建 ttl
nebula> INSERT VERTEX t1(a) values 101:(now());
```

点 101 的 TAG t1 属性会在 now() 之后，经过 5s 后过期。

- 在创建 tag 时设置 TTL。

```ngql
nebula> CREATE TAG t2(a int, b int, c string) ttl_duration= 100, ttl_col = "a";
nebula> INSERT VERTEX t2(a, b, c) values 102:(1584441231, 30, "Word");
```

点 102 的 TAG t2 属性会在 2020年3月17日 18时33分51秒 CST （即时间戳为 1584441231），经过 100s 后过期。

- 当点有多个 TAG 时，各 TAG 的 TTL 相互独立。

```ngql
nebula> CREATE TAG t3(a string);
nebula> INSERT VERTEX t1(a),t3(a) values 200:(now(), "hello");
```

5s 后, 点 Vertex 200 的 t1 属性过期。

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

## 删除 TTL

如果想要删除 TTL，可以设置 `ttl_col` 字段为空，或删除配置的 `ttl_col` 字段，或者设置 `ttl_duration` 为 0 或者 -1。

```ngql
nebula> ALTER TAG t1 ttl_col = ""; -- drop ttl attribute;
```

删除配置的 `ttl_col` 字段：

```ngql
nebula> ALTER TAG t1 DROP (a); -- drop ttl_col
```

设置 ttl_duration 为 0 或者 -1：

```ngql
nebula> ALTER TAG t1 ttl_duration = 0; -- keep the ttl but the data never expires
```

## TTL 使用注意事项

- 如果 `ttl_col` 值为非空，则不支持对 `ttl_col` 值指定的列进行更改操作。

``` ngql
nebula> CREATE TAG t1(a int, b int, c string) ttl_duration = 100, ttl_col = "a";
nebula> ALTER TAG t1 CHANGE (a string); -- failed
```

- 注意一个 tag 或 edge 不能同时拥有 TTL 和索引，只能二者择其一，即使 `ttl_col` 配置的字段与要创建索引的字段不同。

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

- 对 edge 配置 TTL 与 tag 类似。

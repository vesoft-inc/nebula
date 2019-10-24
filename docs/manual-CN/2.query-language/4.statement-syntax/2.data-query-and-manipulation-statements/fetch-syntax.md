# Fetch语句

`FETCH` 语句用于获取点和边的属性。

## 获取点属性

`FETCH PROP ON`可返回节点的一系列属性，目前已支持一条语句返回多个节点属性。

```
FETCH PROP ON <tag_name> <vertex_id_list> [YIELD [DISTINCT] <return_list>]
```

`<tag_name>` 为标签名称，与return_list中的标签相同。

`<vertex_id_list>::=[vertex_id [, vertex_id]]` is a list of vertex id separated by comma(,)

`[YIELD [DISTINCT] <return_list>]` 为返回的属性列表，`YIELD` 语法参看 [YIELD Syntax](yield-syntax.md) 。

### 示例

```SQL
-- 如未指定YIELD字段，则返回节点1的所有属性
nebula> FETCH PROP ON player 1
-- 返回节点1的姓名与年龄属性
nebula> FETCH PROP ON player 1 YIELD player.name, player.age
-- 通过hash生成int64节点id，返回其姓名和年龄属性
nebula> FETCH PROP ON player hash(\"nebula\")  YIELD player.name, player.age
-- 沿边e1寻找节点1的所有近邻，返回其姓名和年龄属性
nebula> GO FROM 1 over e1 | FETCH PROP ON player $- YIELD player.name, player.age
-- 与上述语句相同
nebula> $var = GO FROM 1 over e1; FETCH PROP ON player $var.id YIELD player.name, player.age
-- 获取1，2，3三个节点，返回姓名和年龄都不相同的记录
nebula> FETCH PROP ON player 1,2,3 YIELD DISTINCT player.name, player.age
```

## 获取边属性

使用`FETCH`获取边属性的用法与点属性大致相同，且可同时获取相同类型多条边的属性。

```
FETCH PROP ON <edge_type> <vid> -> <vid> [, <vid> -> <vid> ...] [YIELD [DISTINCT] <return_list>]
```

`<edge_type>` 指定边的类型，需与 `<return_list>` 相同。

`<vid> -> <vid>` 从起始节点到终止节点。多条边需使用逗号隔开。

`[YIELD [DISTINCT] <return_list>]` 为返回的属性列表。

### 获取边属性示例

```SQL
-- 本语句未指定YIELD，因此获取从节点100到节点200边e1的所有属性
nebula> FETCH PROP ON e1 100 -> 200
-- 仅返回属性p1
nebula> FETCH PROP ON e1 100 -> 200 YIELD e1.p1
-- 获取节点1出边e1的prop1属性
nebula> GO FROM 1 OVER e1 YIELD e1.prop1
-- 同上述语句
nebula> GO FROM 1 OVER e1 YIELD e1._src AS s, serve._dst AS d \
 | FETCH PROP ON e1 $-.s -> $-.d YIELD e1.prop1
-- 同上述语句
nebula> $var = GO FROM 1 OVER e1 YIELD e1._src AS s, e2._dst AS d;\
 FETCH PROP ON e3 $var.s -> $var.d YIELD e3.prop1
```

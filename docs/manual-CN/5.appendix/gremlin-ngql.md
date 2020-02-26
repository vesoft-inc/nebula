# Gremlin 和 nGQL 对比

## Gremlin 介绍

[Gremlin](https://tinkerpop.apache.org/gremlin.html) 是 Apache ThinkerPop 框架下的图遍历语言。Gremlin 可以是声明性的也可以是命令性的。虽然 Gremlin 是基于 Groovy 的，但具有许多语言变体，允许开发人员以 Java、JavaScript、Python、Scala、Clojure 和 Groovy 等许多现代编程语言原生编写 Gremlin 查询。

## nGQL 介绍

**Nebula Graph** 的查询语言为 [nGQL](../1.overview/1.concepts/2.nGQL-overview.md)， 是一种类 SQL 的声明型的文本查询语言。相比 SQL，`nGQL` 具有如下特点：

- 类 SQL，易学易用
- 可扩展
- 关键词大小写不敏感
- 支持图遍历
- 支持模式匹配
- 支持聚合运算
- 支持图计算
- 支持分布式事务（开发中）
- 无嵌入支持组合语句，易于阅读

## 基本概念对比

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
vertex, node       | vertex  | vertex        |
edge, relationship | edge    | edge          |
vertex type        | label   | tag           |
edge type          | label   | edge type     |
vertex id          | vid     | vid           |
edge id            | eid     | 无            |

Gremlin 和 nGQL 均使用唯一标识符标记顶点和边。在 **Nebula Graph** 中，用户可以使用指定标识符、哈希或 uuid 函数自动生成标识符。

## 图基本操作

名称                   | Gremlin         | nGQL          |
-----                  |---------        |   -----       |
新建图空间     | g = TinkerGraph.open().traversal() | CREATE SPACE gods |
查看点类型   | g.V().label()   | SHOW TAGS |
插入指定类型点 | g.addV(String vertexLabel).property() | INSERT VERTEX <tag_name> (prop_name_list) VALUES \<vid>:(prop_value_list) |
插入指定类型边 | g.addE(String edgeLabel).from(v1).to(v2).property()| INSERT EDGE <edge_name> ( <prop_name_list> ) VALUES <src_vid> -> <dst_vid>: ( <prop_value_list> ) |
删除点 | g.V(\<vid>).drop() | DELETE VERTEX \<vid> |
删除边  | g.E(\<vid>).outE(\<type>).where(otherV().is(\<vid>))drop() | DELETE EDGE <edge_type> \<src_vid> -> \<dst_vid> |
更新点属性 | g.V(\<vid>).property() | UPDATE VERTEX \<vid> SET <update_columns> |
查看指定点 | g.V(\<vid>) | FETCH PROP ON <tag_name> \<vid>|
查看指定边 | g.E(<src_vid> >> <dst_vid>) | FETCH PROP ON <edge_name> <src_vid> -> <dst_vid> |
沿指定点查询指定边 | g.V(\<vid>).outE( \<edge>) | GO FROM \<vid> OVER  \<edge> |
沿指定点反向查询指定边 | g.V(\<vid>).in( \<edge>) | GO FROM \<vid>  OVER \<edge> REVERSELY |
沿指定点查询指定边 N 跳 | g.V(\<vid>).repeat(out(\<edge>)).times(N) | GO N STEPS FROM \<vid> OVER \<edge> |
返回指定两点路径 | g.V(\<vid>).repeat(out()).until(\<vid>).path() | FIND ALL PATH FROM \<vid> TO \<vid> OVER * |

## 示例查询

本节中的示例使用了 [Janus Graph](https://janusgraph.org/) 的示例图 [_The Graphs of Gods_](https://docs.janusgraph.org/#getting-started)。该图结构如下图所示。此处使用[属性图模型](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-ZH/1.overview/1.concepts/1.data-model.md)描述罗马万神话中诸神关系。

![image](https://user-images.githubusercontent.com/42762957/71503167-0e264b80-28af-11ea-87c5-76f4fd1275cd.png)

- 插入数据
  
```bash
# 插入点
nebula> INSERT VERTEX character(name, age, type) VALUES hash("saturn"):("saturn", 10000, "titan"), hash("jupiter"):("jupiter", 5000, "god");

gremlin> saturn = g.addV("character").property(T.id, 1).property('name', 'saturn').property('age', 10000).property('type', 'titan').next();
==>v[1]
gremlin> jupiter = g.addV("character").property(T.id, 2).property('name', 'jupiter').property('age', 5000).property('type', 'god').next();
==>v[2]
gremlin> prometheus = g.addV("character").property(T.id, 31).property('name',  'prometheus').property('age', 1000).property('type', 'god').next();
==>v[31]
gremlin> jesus = g.addV("character").property(T.id, 32).property('name',  'jesus').property('age', 5000).property('type', 'god').next();
==>v[32]

# 插入边
nebula> INSERT EDGE father() VALUES hash("jupiter")->hash("saturn"):();
gremlin> g.addE("father").from(jupiter).to(saturn).property(T.id, 13);
==>e[13][2-father->1]
```

- 删除数据
  
```bash
nebula> DELETE VERTEX hash("prometheus");
gremlin> g.V(prometheus).drop();
```

- 更新数据

```bash
nebula> UPDATE VERTEX hash("jesus") SET character.type = 'titan';
gremlin> g.V(jesus).property('age', 6000);
```

- 查看数据
  
```bash
nebula> FETCH PROP ON character hash("saturn");
===================================================
| character.name | character.age | character.type |
===================================================
| saturn         | 10000         | titan          |
---------------------------------------------------

gremlin> g.V(saturn).valueMap();
==>[name:[saturn],type:[titan],age:[10000]]
```

- 查询 hercules 的祖父

```bash
nebula> GO 2 STEPS FROM hash("hercules") OVER father YIELD $$.character.name;
=====================
| $$.character.name |
=====================
| saturn            |
---------------------

gremlin> g.V().hasLabel('character').has('name','hercules').out('father').out('father').values('name');
==>saturn
```

- 查询 hercules 的父亲

```bash
nebula> GO FROM hash("hercules") OVER father YIELD $$.character.name;
=====================
| $$.character.name |
=====================
| jupiter           |
---------------------

gremlin> g.V().hasLabel('character').has('name','hercules').out('father').values('name');
==>jupiter
```

- 查询年龄大于 100 的人物

```bash
nebula> LOOKUP ON character WHERE character.age > 100 YIELD character.name AS name;

gremlin> g.V().hasLabel('character').has('age',gt(100)).values('name');
==>saturn
==>jupiter
==>neptune
==>pluto
```

- 查询和 pluto 一起居住的人物

```bash
nebula> GO FROM hash("pluto") OVER lives YIELD lives._dst AS place | \
GO FROM $-.place OVER lives REVERSELY YIELD $$.character.name AS cohabitants;
===============
| cohabitants |
===============
| pluto       |
---------------
| cerberus    |
---------------

gremlin> g.V(pluto).out('lives').in('lives').values('name');
==>pluto
==>cerberus
```

- 从一起居住的人物中排除 pluto 本人

```bash
nebula> GO FROM hash("pluto") OVER lives YIELD lives._dst AS place | GO FROM $-.place OVER lives REVERSELY WHERE \
$$.character.name != "pluto" YIELD $$.character.name AS cohabitants;
===============
| cohabitants |
===============
| cerberus    |
---------------

gremlin> g.V(pluto).out('lives').in('lives').where(is(neq(pluto))).values('name');
==>cerberus
```

- Pluto 的兄弟们

```bash
# where do pluto's brothers live?

nebula> GO FROM hash("pluto") OVER brother YIELD brother._dst AS brother | \
GO FROM $-.brother OVER lives YIELD $$.location.name;
====================
| $$.location.name |
====================
| sky              |
--------------------
| sea              |
--------------------

gremlin> g.V(pluto).out('brother').out('lives').values('name');
==>sky
==>sea

# which brother lives in which place?

nebula> GO FROM hash("pluto") OVER brother YIELD brother._dst AS god | \
GO FROM $-.god OVER lives YIELD $^.character.name AS Brother, $$.location.name AS Habitations;
=========================
| Brother | Habitations |
=========================
| jupiter | sky         |
-------------------------
| neptune | sea         |
-------------------------

gremlin> g.V(pluto).out('brother').as('god').out('lives').as('place').select('god','place').by('name');
==>[god:jupiter, place:sky]
==>[god:neptune, place:sea]
```

## 高级查询

### 边的遍历

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
指定点沿指定边的出顶点 | out(\<label>)       | GO FROM \<vertex_id> OVER \<edge_type>  |
指定点沿指定边的入顶点 | in(\<label>)    | GO FROM \<vertex_id> OVER \<edge_type> REVERSELY          |
指定点沿指定边的双向顶点      | both(\<label>)   | GO FROM \<vertex_id> OVER \<edge_type> BIDIRECT           |

```bash
# 访问某个顶点沿某条边的 OUT 方向邻接点
gremlin> g.V(jupiter).out('brother');
nebula> GO FROM hash("jupiter") OVER brother;

# 访问某个顶点沿某条边的 IN 方向邻接点
gremlin> g.V(jupiter).in('brother');
nebula> GO FROM hash("jupiter") OVER brother REVERSELY;

# 访问某个顶点沿某条边的双向邻接点
gremlin> g.V(jupiter).both('brother');
nebula> GO FROM hash("jupiter") OVER brother BIDIRECT;

# 2度 out 查询
gremlin> g.V(hercules).out('father').out('lives');
nebula> GO FROM hash("hercules") OVER father YIELD father._dst AS id | \
GO FROM $-.id OVER lives;
```

### has 条件过滤

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
通过 ID 来过滤顶点 | hasId(\<vertex_id>)       | FETCH PROP ON \<vertex_id> |
通过 label 和属性的名字和值过滤顶点和边  | has(\<label>, \<key>, \<value>)    | LOOKUP \<tag> \| \<edge_type> WHERE \<expression>        |

```bash
# 查询 ID 为 saturn 的顶点
gremlin> g.V().hasId(saturn);
nebula> FETCH PROP ON * hash("saturn");

# 查询 tag 为 character 且 name 属性值为 hercules 的顶点

gremlin> g.V().has('character','name','hercules').valueMap();
nebula> LOOKUP character WHERE character.name == 'hercules' YIELD character.name, character.age, character.type;
```

### 返回结果限制

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
指定返回结果行数  | limit()    | LIMIT        |
获取后 n 个元素 | tail() | ORDER BY \<expression> DESC LIMIT |
跳过前 n 个元素 | skip() | LIMIT \<offset_value> |

```bash
# 查询前两个顶点
gremlin> g.V().has('character','name','hercules').out('battled').limit(2);
nebula> GO FROM hash('hercules') OVER battled | LIMIT 2;

# 查询后两个顶点
gremlin> g.V().has('character','name','hercules').out('battled').values('name').tail(1);
nebula> GO FROM hash('hercules') OVER battled YIELD $$.character.name AS name | ORDER BY name DESC | LIMIT 1;

# 跳过第 1 个元素并返回一个元素
gremlin> g.V().has('character','name','hercules').out('battled').values('name').skip(1).limit(1);
nebula> GO FROM hash('hercules') OVER battled YIELD $$.character.name AS name | ORDER BY name | LIMIT 1,1;
```

### 路径查询

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
所有路径  | path()    | FIND ALL PATH        |
不包含环路 | simplePath() | \ |
只包含环路 | cyclicPath() | \ |
最短路径   | \    | FIND SHORTEST PATH |

**注意：**  **Nebula Graph** 需要起始点和终点方可返回路径， Gremlin 仅需要起始点。

```bash
# pluto 顶点到与其有直接关联的出边顶点的路径
gremlin> g.V().hasLabel('character').has('name','pluto').out().path();
# 查询点 pluto 到点 jupiter 的最短路径
nebula> LOOKUP ON character WHERE character.name== "pluto" YIELD character.name AS name | \
    FIND SHORTEST PATH FROM $-.VertexID TO hash("jupiter") OVER *;
```

### 多度查询

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
指定重复执行的语句  | repeat()    | N STEPS        |
指定重复执行的次数 | times() | N STEPS |
指定循环终止的条件 | until() | \ |
指定收集数据的条件   | emit()    | \ |

```bash
# 查询点 pluto 出边邻点
gremlin> g.V().hasLabel('character').has('name','pluto').repeat(out()).times(1);
nebula> LOOKUP ON character WHERE character.name== "pluto" YIELD character.name AS name | \
    GO FROM $-.VertexID OVER *;

# 查询顶点 hercules 到顶点 cerberus 之间的路径
# 循环的终止条件是遇到名称是 cerberus 的顶点
gremlin> g.V().hasLabel('character').has('name','hercules').repeat(out()).until(has('name', 'cerberus')).path();
nebula> # Coming soon

# 查询点 hercules 的所有出边可到达点的路径
# 且终点必须是 character 类型的点
gremlin> g.V().hasLabel('character').has('name','hercules').repeat(out()).emit(hasLabel('character')).path();
nebula> # Coming soon

# 查询两顶点 pluto 和 saturn 之间的最短路径
# 且最大深度为 3
gremlin> g.V('pluto').repeat(out().simplePath()).until(hasId('saturn').and().loops().is(lte(3))).hasId('saturn').path();
nebula> FIND SHORTEST PATH FROM hash('pluto') TO hash('saturn') OVER * UPTO 3 STEPS;
```

### 查询结果排序

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
升序排列 | order().by()    | ORDER BY         |
降序排列 | order().by(decr) | ORDER BY DESC |
随机排列 | order().by(shuffle) | \ |

```bash
# 查询 pluto 的兄弟并按照年龄降序排列
gremlin> g.V(pluto).out('brother').order().by('age', decr).valueMap();
nebula> GO FROM hash('pluto') OVER brother YIELD $$.character.name AS Name, $$.character.age as Age | ORDER BY Age DESC;
```

### Group By

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
对结果集进行分组 | group().by()    | GROUP BY         |
去除相同元素 | dedup() | \ |
对结果集进行分组并统计 | groupCount() | GROUP BY COUNT  |

**注意：** GROUP BY 函数只能与 YIELD 语句一起使用。

```bash
# 根据顶点类别进行分组并统计各个类别的数量
gremlin> g.V().group().by(label).by(count());
nebula> # coming soon

# 查询点 jupiter 出边邻点，使用 name 分组并统计
gremlin> g.V(jupiter).out().group().by('name').by(count());
nebula> GO FROM hash('jupiter') OVER * YIELD $$.character.name AS Name, $$.character.age as Age, $$.location.name | \
GROUP BY $-.Name YIELD $-.Name, COUNT(*);
```

### where 条件过滤

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
where 条件过滤 | where()    | WHERE         |

过滤条件对比：

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
等于 | eq(object)    | ==         |
不等于 | neq(object)   | !=         |
小于 | lt(number)    | <         |
小于等于 | lte(number)    | <=        |
大于 | gt(number)    | >       |
大于等于 | gte(number)    | >=         |
判断值是否在指定的列表中 | within(objects…​)    | udf_is_in()         |

```bash
gremlin> eq(2).test(3);
nebula> YIELD 3 == 2;

gremlin> within('a','b','c').test('d');
nebula> YIELD udf_is_in('d', 'a', 'b', 'c');
```

```bash
# 找出 pluto 和谁住并排队他本人
gremlin> g.V(pluto).out('lives').in('lives').where(is(neq(pluto))).values('name');
nebula> GO FROM hash("pluto") OVER lives YIELD lives._dst AS place | GO FROM $-.place OVER lives REVERSELY WHERE \
$$.character.name != "pluto" YIELD $$.character.name AS cohabitants;
```

### 逻辑运算

名称               | Gremlin | nGQL           |
-----              |---------|   -----       |
Is | is()    | ==         |
Not | not()    | !=         |
And | and()    | AND         |
Or | or()    | OR         |

```bash
# 查询年龄大于 30 的人物
gremlin> g.V().values('age').is(gte(30));
nebula> LOOKUP ON character WHERE character.age >= 30 YIELD character.age;

# 查询名称为 pluto 且年龄为 4000 的人物
gremlin> g.V().has('name','pluto').and().has('age',4000);
nebula> LOOKUP ON character WHERE character.name == 'pluto' AND character.age == 4000;

# 逻辑非的用法
gremlin> g.V().has('name','pluto').out('brother').not(values('name').is('neptune')).values('name');
nebula> LOOKUP ON character WHERE character.name == 'pluto' YIELD character.name AS name | \
GO FROM $-.VertexID OVER brother WHERE $$.character.name != 'neptune' YIELD $$.character.name;
```

### 模式匹配

TODO

<!-- ## References

Multiple properties (multi-properties): a vertex property key can have multiple values. For example, a vertex can have multiple "name" properties.

create this Graph and establish a TraversalSource with

```bash
graph = TinkerFactory.createTheCrew()
g = graph.traversal()
g.V().hasLabel('person').valueMap()    //get a basic feeling for the data of the "person" vertices in the graph
g.V().group().by(label)
```

```bash
#用内置的TinkerGraph-db创建一个空的图对象
$ gremlin> g = TinkerGraph.open().traversal()
```

When running in the Gremlin Console, support for TinkerGraph should be on by default. Create a new TinkerGraph instance from the console as follows.

```bash
graph = TinkerGraph.open()
```

Before you can start to issue Gremlin queries against the graph you also need to establish a graph traversal source object by calling the new graph’s traversal method as follows.

```bash
g = graph.traversal()
// Tell me something about my graph
graph.toString()
``` -->

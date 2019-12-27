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

  gremlin> v1 = g.addV("character").property(T.id, 1).property('name', 'saturn').property('age', 10000).property('type', 'titan').next();
  ==>v[1]
  gremlin> v2 = g.addV("character").property(T.id, 2).property('name','jupiter').property('age', 5000).property('type', 'god').next();
  ==>v[2]
  gremlin> v3 = g.addV("character").property(T.id, 3).property('name',  'prometheus').property('age', 1000).property('type', 'god').next();
  ==>v[3]
  gremlin> v4 = g.addV("character").property(T.id, 4).property('name',  'jesus').property('age', 5000).property('type', 'god').next();
  ==>v[4]

  # 插入边
  nebula> INSERT EDGE father() VALUES hash("jupiter")->hash("saturn"):();
  gremlin> g.addE("father").from(v1).to(v2).property(T.id, 3);
  ==>e[3][1-father->2]
  ```

- 删除数据
  
  ```bash
  nebula> DELETE VERTEX hash("prometheus");
  gremlin> g.V(v3).drop();
  ```

- 更新数据

```bash
nebula> UPDATE VERTEX hash("jesus") SET character.type = 'titan';
gremlin> g.V(v4).property('age', 6000);
```

- 查看数据
  
  ```bash
  nebula> FETCH PROP ON character hash("saturn");
  ===================================================
  | character.name | character.age | character.type |
  ===================================================
  | saturn         | 10000         | titan          |
  ---------------------------------------------------

  gremlin> g.V(1).valueMap();
  ==>[name:[saturn],type:[titan],age:[10000]]
  ```

- 查询 hercules 的祖父

    ```bash
    nebula> GO 2 STEPS FROM hash("hercules") OVER father YIELD  $$.character.name;
    =====================
    | $$.character.name |
    =====================
    | saturn            |
    ---------------------

    gremlin> g.V().hasLabel('character').has('name','hercules').out ('father').out('father').values('name');
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

    gremlin> g.V().hasLabel('character').has('name','hercules').out ('father').values('name');
    ==>jupiter
    ```

- 查询年龄大于 100 的人物

    ```bash
    nebula> XXX # not supported yet
    gremlin> g.V().hasLabel('character').has('age',gt(100));
    ```

- 查询和 pluto 一起居住的人物

    ```bash
    nebula> GO FROM hash("pluto") OVER lives \
            YIELD lives._dst AS place | \
            GO FROM $-.place OVER lives REVERSELY YIELD \
            $$.character.name AS cohabitants;
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
    nebula>  GO FROM hash("pluto") OVER lives YIELD lives._dst AS place | \
                        GO FROM $-.place OVER lives REVERSELY WHERE \
                        $$.character.name != "pluto" YIELD \
                        $$.character.name AS cohabitants;
    ===============
    | cohabitants |
    ===============
    | cerberus    |
    ---------------

    gremlin> g.V(pluto).out('lives').in('lives').where(is(neq(pluto)))  .values('name');
    ==>cerberus
    ```

- Pluto 的兄弟们

    ```bash
    # where do pluto's brothers live?

    nebula> GO FROM hash("pluto") OVER brother \
                        YIELD brother._dst AS brother | \
                        GO FROM $-.brother OVER lives \
                        YIELD $$.location.name;
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

    nebula> GO FROM hash("pluto") OVER brother \
            YIELD brother._dst AS god | \
            GO FROM $-.god OVER lives YIELD \
            $^.character.name AS \
            Brother, $$.location.name AS \
            Habitations;
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

# Comparison Between Gremlin and nGQL

## Introduction to Gremlin

Gremlin is a graph traversal language developed by Apache TinkerPop. It can be either declarative or imperative. Gremlin is Groovy-based, but has many language variants that allow developers to write Gremlin queries natively in many modern programming languages such as Java, JavaScript, Python, Scala, Clojure and Groovy.

## Introduction to nGQL

**Nebula Graph** introduces its own query language, `nGQL`, which a declarative, textual query language like SQL, but for graphs. Unlike SQL, `nGQL` is all about expressing graph patterns. The features of `nGQL` is as follows:

- Syntax is close to SQL, but not exactly the same (Easy to learn)
- Expandable
- Case insensitive
- Support basic graph traverse
- Support pattern match
- Support aggregation
- Support graph mutation
- Support distributed transaction (future release)
- Statement composition, but **NO** statement embedding (Easy to read)

## Conceptual Comparisons

Name               | Gremlin | nGQL          |
-----              |---------|   -----       |
vertex, node       | vertex  | vertex        |
edge, relationship | edge    | edge          |
vertex type        | label   | tag           |
edge type          | label   | edge type     |

In Gremlin and nGQL, vertices and edges are identified with unique identifiers. In **Nebula Graph**, you can either specify identifiers or generate automatically with the hash or uuid function.

## Basic Graph Operations

Name                   | Gremlin         | nGQL          |
-----                  |---------        |   -----       |
Create a new graph     | g = TinkerGraph.open().traversal() | CREATE SPACE gods |
Show vertices' types   | g.V().label()   | SHOW TAGS |
Insert a vertex with specified type | g.addV(String vertexLabel).property() | INSERT VERTEX <tag_name> (prop_name_list) VALUES vid:(prop_value_list) |
Insert an edge with specified edge type | g.addE(String edgeLabel).from(v1).to(v2).property()| INSERT EDGE <edge_name> ( <prop_name_list> ) VALUES <src_vid> -> <dst_vid>: ( <prop_value_list> ) |
Delete a vertex | g.V(\<vid>).drop() | DELETE VERTEX \<vid> |
Delete an edge  | g.E(\<vid>).outE(\<type>).where(otherV().is(\<vid>))drop() | DELETE EDGE <edge_type> \<vid> -> \<vid> |
Update a vertex property | g.V(\<vid>).property() | UPDATE VERTEX <vid> SET <update_columns> |
Fetch vertices with ID | g.V(\<vid>) | FETCH PROP ON <tag_name> \<vid>|
Fetch edges with ID    | g.E(<start_vid> >> <dst_vid>) | FETCH PROP ON <edge_name> <start_vid> -> <dst_vid> |
Query a vertex along specified edge type | g.V(\<vid>).outE( \<edge>) | GO FROM \<vid> OVER  \<edge> |
Query a vertex along specified edge type reversely | g.V(\<vid>).in( \<edge>) | GO FROM \<vid>  OVER \<edge> REVERSELY |
Query N hops along a specified edge | g.V(\<vid>).repeat(out(\<edge>)).times(N) | GO N STEPS FROM \<vid> OVER \<edge> |
Find path between two vertices | g.V(\<vid>).repeat(out()).until(\<vid>).path() | FIND ALL PATH FROM \<vid> TO \<vid> OVER * |

## Example Queries

The examples in this section make extensive use of the toy graph distributed with Titan called _The Graph of the Gods_. This graph is diagrammed below. The abstract data model is known as a [Property Graph Model](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/1.overview/1.concepts/1.data-model.md) and this particular instance describes the relationships between the beings and places of the Roman pantheon.

![image](https://user-images.githubusercontent.com/42762957/71347155-a1f0d280-25a4-11ea-98f0-d1754e9ea3e4.png)

- Insert data
  
  ```bash
  # insert vertex
  nebula> INSERT VERTEX character(name, age, type) VALUES hash("saturn"):("saturn", 10000, "titan");
  nebula> INSERT VERTEX character(name, age, type) VALUES hash("jupiter"):("jupiter", 5000, "god");

  gremlin> v1 = g.addV("character").property(T.id, 1).property('name', 'saturn').property('age', 10000).property('type', 'titan');
  ==>v[1]
  gremlin> v2 = g.addV("character").property(T.id, 2)property('name','jupiter').property('age', 5000).property('type', 'god');
  ==>v[2]

  # insert edge
  nebula> INSERT EDGE father() VALUES hash("jupiter")->hash("saturn"):();
  gremlin> g.addE("father").from(v1).to(v2).property(T.id, 3)
  ==>e[3][1-father->2]
  ```

- Delete data
  
  ```bash
  nebula> DELETE VERTEX hash("prometheus")
  gremlin> g.V(v3).drop()
  ```

- Update vertex

```bash
nebula> UPDATE VERTEX hash("jesus") SET character.type = 'titan';
gremlin> g.V(v7).property('age', 6000)
```

- Fetch data
  
  ```bash
  nebula> FETCH PROP ON character hash("saturn");
  ===================================================
  | character.name | character.age | character.type |
  ===================================================
  | saturn         | 10000         | titan          |
  ---------------------------------------------------

  gremlin> g.V(1).valueMap()
  ==>[name:[saturn],type:[titan],age:[10000]]
  ```

- Find the name of hercules's grandfather

    ```bash
    nebula> GO 2 STEPS FROM hash("hercules") OVER father YIELD  $$.character.name
    =====================
    | $$.character.name |
    =====================
    | saturn            |
    ---------------------

    gremlin> g.V().hasLabel('character').has('name','hercules').out ('father').out('father').value('name')
    ==>saturn
    ```

- Find the name of hercules's father

    ```bash
    nebula> GO FROM hash("hercules") OVER father YIELD $$.character.name
    =====================
    | $$.character.name |
    =====================
    | jupiter           |
    ---------------------

    gremlin> g.V().hasLabel('character').has('name','hercules').out ('father').value('name')
    ==>jupiter
    ```

- Find the characters with age > 100

    ```bash
    nebula> XXX # not supported yet
    gremlin> g.V().hasLabel('character').has('age',gt(100))
    ```

- Find who are pluto's cohabitants

    ```bash
    nebula> GO FROM hash("pluto") OVER lives YIELD lives._dst AS place | \
                          GO FROM $-.place OVER lives REVERSELY YIELD \
                          $$.character.name AS cohabitants
    ===============
    | cohabitants |
    ===============
    | pluto       |
    ---------------
    | cerberus    |
    ---------------

    gremlin> g.V(pluto).out('lives').in('lives').values('name')
    ==>pluto
    ==>cerberus
    ```

- pluto can't be his own cohabitant

    ```bash
    nebula>  GO FROM hash("pluto") OVER lives YIELD lives._dst AS place | \
                        GO FROM $-.place OVER lives REVERSELY WHERE \
                        $$.character.name != "pluto" YIELD \
                        $$.character.name AS cohabitants
    ===============
    | cohabitants |
    ===============
    | cerberus    |
    ---------------

    gremlin> g.V(pluto).out('lives').in('lives').where(is(neq(pluto)))  .values('name')
    ==>cerberus
    ```

- Pluto's Brothers

    ```bash
    # where do pluto's brothers live?

    nebula> GO FROM hash("pluto") OVER brother \
                        YIELD brother._dst AS brother | \
                        GO FROM $-.brother OVER lives \
                        YIELD $$.location.name  
    ====================
    | $$.location.name |
    ====================
    | sky              |
    --------------------
    | sea              |
    --------------------

    gremlin> g.V(pluto).out('brother').out('lives').values('name')
    ==>sky
    ==>sea

    # which brother lives in which place?

    nebula> GO FROM hash("pluto") OVER brother \
                          YIELD brother._dst AS god | \
                          GO FROM $-.god OVER lives YIELD \
                          $^.character.name AS Brother, $$.location.name AS     Habitations
    =========================
    | Brother | Habitations |
    =========================
    | jupiter | sky         |
    -------------------------
    | neptune | sea         |
    -------------------------

    gremlin> g.V(pluto).out('brother').as('god').out('lives').as('place')   .select('god','place').by('name')
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

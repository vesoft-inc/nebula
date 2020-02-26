# Comparison Between Gremlin and nGQL

## Introduction to Gremlin

[Gremlin](https://tinkerpop.apache.org/gremlin.html) is a graph traversal language developed by Apache TinkerPop. It can be either declarative or imperative. Gremlin is Groovy-based, but has many language variants that allow developers to write Gremlin queries natively in many modern programming languages such as Java, JavaScript, Python, Scala, Clojure and Groovy.

## Introduction to nGQL

**Nebula Graph** introduces its own query language, [nGQL](../1.overview/1.concepts/2.nGQL-overview.md), which is a declarative, textual query language like SQL, but for graphs. Unlike SQL, `nGQL` is all about expressing graph patterns. The features of `nGQL` are as follows:

- Syntax is close to SQL, but not exactly the same (Easy to learn)
- Expandable
- Keyword is case insensitive
- Support basic graph traverse
- Support pattern matching
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
vertex id          | vid     | vid           |
edge id            | eid     | not support   |

In Gremlin and nGQL, vertices and edges are identified with unique identifiers. In **Nebula Graph**, you can either specify identifiers or generate automatically with the hash or uuid function.

## Basic Graph Operations

Name                   | Gremlin         | nGQL          |
-----                  |---------        |   -----       |
Create a new graph     | g = TinkerGraph.open().traversal() | CREATE SPACE gods |
Show vertices' types   | g.V().label()   | SHOW TAGS |
Insert a vertex with a specified type | g.addV(String vertexLabel).property() | INSERT VERTEX <tag_name> (prop_name_list) VALUES \<vid>:(prop_value_list) |
Insert an edge with specified edge type | g.addE(String edgeLabel).from(v1).to(v2).property()| INSERT EDGE <edge_name> ( <prop_name_list> ) VALUES <src_vid> -> <dst_vid>: ( <prop_value_list> ) |
Delete a vertex | g.V(\<vid>).drop() | DELETE VERTEX \<vid> |
Delete an edge  | g.E(\<vid>).outE(\<type>).where(otherV().is(\<vid>))drop() | DELETE EDGE <edge_type> \<src_vid> -> \<dst_vid> |
Update a vertex property | g.V(\<vid>).property() | UPDATE VERTEX \<vid> SET <update_columns> |
Fetch vertices with ID | g.V(\<vid>) | FETCH PROP ON <tag_name> \<vid>|
Fetch edges with ID    | g.E(<src_vid> >> <dst_vid>) | FETCH PROP ON <edge_name> <src_vid> -> <dst_vid> |
Query a vertex along specified edge type | g.V(\<vid>).outE( \<edge>) | GO FROM \<vid> OVER  \<edge> |
Query a vertex along specified edge type reversely | g.V(\<vid>).in( \<edge>) | GO FROM \<vid>  OVER \<edge> REVERSELY |
Query N hops along a specified edge | g.V(\<vid>).repeat(out(\<edge>)).times(N) | GO N STEPS FROM \<vid> OVER \<edge> |
Find path between two vertices | g.V(\<vid>).repeat(out()).until(\<vid>).path() | FIND ALL PATH FROM \<vid> TO \<vid> OVER * |

## Example Queries

The examples in this section make extensive use of the toy graph distributed with [Janus Graph](https://janusgraph.org/) called [_The Graphs of Gods_](https://docs.janusgraph.org/#getting-started). This graph is diagrammed below. The abstract data model is known as a [Property Graph Model](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/1.overview/1.concepts/1.data-model.md) and this particular instance describes the relationships between the beings and places of the Roman pantheon.

![image](https://user-images.githubusercontent.com/42762957/71503167-0e264b80-28af-11ea-87c5-76f4fd1275cd.png)

- Insert data
  
```bash
# insert vertex
nebula> INSERT VERTEX character(name,age, type) VALUES hash("saturn"):("saturn", 10000, "titan"), hash("jupiter"):("jupiter", 5000, "god");

gremlin> saturn = g.addV("character").property(T.id, 1).property('name', 'saturn').property('age', 10000).property('type', 'titan').next();
==>v[1]
gremlin> jupiter = g.addV("character").property(T.id, 2).property('name', 'jupiter').property('age', 5000).property('type', 'god').next();
==>v[2]
gremlin> prometheus = g.addV("character").property(T.id, 31).property('name',  'prometheus').property('age', 1000).property('type', 'god').next();
==>v[31]
gremlin> jesus = g.addV("character")property(T.id, 32).property('name', 'jesus').property('age', 5000).property('type', 'god').next();
==>v[32]

# insert edge
nebula> INSERT EDGE father() VALUES hash("jupiter")->hash("saturn"):();
gremlin> g.addE("father").from(jupiter).to(saturn).property(T.id, 13);
==>e[13][2-father->1]
```

- Delete vertex
  
```bash
nebula> DELETE VERTEX hash("prometheus");
gremlin> g.V(prometheus).drop();
```

- Update vertex

```bash
nebula> UPDATE VERTEX hash("jesus") SET character.type = 'titan';
gremlin> g.V(jesus).property('age', 6000);
```

- Fetch data
  
```bash
nebula> FETCH PROP ON character hash("saturn");
==================================================
| character.name | character.age |character.type |
==================================================
| saturn         | 10000         |titan          |
--------------------------------------------------

gremlin> g.V(saturn).valueMap();
==>[name:[saturn],type:[titan],age:[10000]]
```

- Find the name of hercules's grandfather

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

- Find the name of hercules's father

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

- Find the characters with age > 100

```bash
nebula> LOOKUP ON character WHERE character.age > 100 YIELD character.name AS name;

gremlin> g.V().hasLabel('character').has('age',gt(100)).values('name');
==>saturn
==>jupiter
==>neptune
==>pluto
```

- Find who are pluto's cohabitants

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

- pluto can't be his own cohabitant

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

- Pluto's Brothers

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

## Advance Queries

### Traversing Edges

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Out adjacent vertices to the vertex | out(\<label>)       | GO FROM \<vertex_id> OVER \<edge_type>  |
In adjacent vertices to the vertex | in(\<label>)    | GO FROM \<vertex_id> OVER \<edge_type> REVERSELY          |
Both adjacent vertices of the vertex      | both(\<label>)   | GO FROM \<vertex_id> OVER \<edge_type> BIDIRECT           |

```bash
# Find the out adjacent vertices of a vertex along an edge
gremlin> g.V(jupiter).out('brother');
nebula> GO FROM hash("jupiter") OVER brother;

# Find the in adjacent vertices of a vertex along an edge
gremlin> g.V(jupiter).in('brother');
nebula> GO FROM hash("jupiter") OVER brother REVERSELY;

# Find the both adjacent vertices of a vertex along an edge
gremlin> g.V(jupiter).both('brother');
nebula> GO FROM hash("jupiter") OVER brother BIDIRECT;

# Two hops out traverse
gremlin> g.V(hercules).out('father').out('lives');
nebula> GO FROM hash("hercules") OVER father YIELD father._dst AS id | \
GO FROM $-.id OVER lives;
```

### Has Filter Condition

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Filter vertex via identifier | hasId(\<vertex_id>)       | FETCH PROP ON \<vertex_id> |
Filter vertex or edge via label, key and value  | has(\<label>, \<key>, \<value>)    | LOOKUP \<tag> \| \<edge_type> WHERE \<expression>        |

```bash
# Filter vertex with ID saturn
gremlin> g.V().hasId(saturn);
nebula> FETCH PROP ON * hash("saturn");

# Find for vertices with tag "character" and "name" attribute value "hercules"

gremlin> g.V().has('character','name','hercules').valueMap();
nebula> LOOKUP character WHERE character.name == 'hercules' YIELD character.name, character.age, character.type;
```

### Limiting Returned Results

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Constrain the number of rows to return  | limit()    | LIMIT        |
Emit the last n-objects | tail() | ORDER BY \<expression> DESC LIMIT |
Skip n-objects | skip() | LIMIT \<offset_value> |

```bash
# Find the first two records
gremlin> g.V().has('character','name','hercules').out('battled').limit(2);
nebula> GO FROM hash('hercules') OVER battled | LIMIT 2;

# Find the last record
gremlin> g.V().has('character','name','hercules').out('battled').values('name').tail(1);
nebula> GO FROM hash('hercules') OVER battled YIELD $$.character.name AS name | ORDER BY name DESC | LIMIT 1;

# Skip the first record and return one record
gremlin> g.V().has('character','name','hercules').out('battled').values('name').skip(1).limit(1);
nebula> GO FROM hash('hercules') OVER battled YIELD $$.character.name AS name | ORDER BY name | LIMIT 1,1;
```

### Finding Path

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
All path  | path()    | FIND ALL PATH        |
Exclude cycles path | simplePath() | \ |
Only cycles path | cyclicPath() | \ |
Shortest path   | \    | FIND SHORTEST PATH |

**NOTE:** **Nebula Graph** requires the source vertex and the dest vertex to find path while Gremlin only needs the source vertex.

```bash
# Find path from vertex pluto to the out adjacent vertices
gremlin> g.V().hasLabel('character').has('name','pluto').out().path();
# Find the shortest path from vertex pluto to vertex jupiter
nebula> LOOKUP ON character WHERE character.name== "pluto" YIELD character.name AS name | \
    FIND SHORTEST PATH FROM $-.VertexID TO hash("jupiter") OVER *;
```

### Traversing N Hops

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Loop over a traversal  | repeat()    | N STEPS        |
Times the traverser has gone through a loop | times() | N STEPS |
Specify when to end the loop | until() | \ |
Specify when to collect data   | emit()    | \ |

```bash
# Find vertex pluto's out adjacent neighbors
gremlin> g.V().hasLabel('character').has('name','pluto').repeat(out()).times(1);
nebula> LOOKUP ON character WHERE character.name== "pluto" YIELD character.name AS name | \
    GO FROM $-.VertexID OVER *;

# Find path between vertex hercules and vertex cerberus
# Stop traversing when the dest vertex is cerberus
gremlin> g.V().hasLabel('character').has('name','hercules').repeat(out()).until(has('name', 'cerberus')).path();
nebula> # Coming soon

# Find path sourcing from vertex hercules
# And the dest vertex type is character
gremlin> g.V().hasLabel('character').has('name','hercules').repeat(out()).emit(hasLabel('character')).path();
nebula> # Coming soon

# Find shortest path between pluto and saturn over any edge
# And the deepest loop is 3
gremlin> g.V('pluto').repeat(out().simplePath()).until(hasId('saturn').and().loops().is(lte(3))).hasId('saturn').path();
nebula> FIND SHORTEST PATH FROM hash('pluto') TO hash('saturn') OVER * UPTO 3 STEPS;
```

### Ordering Results

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Order the items increasingly | order().by()    | ORDER BY         |
Order the items decreasingly | order().by(decr) | ORDER BY DESC |
Randomize the records order | order().by(shuffle) | \ |

```bash
# Find pluto's brother and order by age decreasingly.
gremlin> g.V(pluto).out('brother').order().by('age', decr).valueMap();
nebula> GO FROM hash('pluto') OVER brother YIELD $$.character.name AS Name, $$.character.age as Age | ORDER BY Age DESC;
```

### Group By

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Group by items | group().by()    | GROUP BY         |
Remove repeated items | dedup() | \ |
Group by items and count | groupCount() | GROUP BY COUNT  |

**Note:** The GROUP BY functions can only be applied in the YIELD clause.

```bash
# Group vertices by label then count
gremlin> g.V().group().by(label).by(count());
nebula> # coming soon

# Find vertex jupiter's out adjacency vertices, group by name, then count
gremlin> g.V(jupiter).out().group().by('name').by(count());
nebula> GO FROM hash('jupiter') OVER * YIELD $$.character.name AS Name, $$.character.age as Age, $$.location.name | \
GROUP BY $-.Name YIELD $-.Name, COUNT(*);
```

### Where Filter Condition

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Where filter condition | where()    | WHERE         |

Predicates comparison:

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Equal to | eq(object)    | ==         |
Not equal to | neq(object)   | !=         |
Less than | lt(number)    | <         |
Less than or equal to | lte(number)    | <=        |
Greater than | gt(number)    | >       |
Greater than or equal to | gte(number)    | >=         |
Whether a value is within the array | within(objects…​)    | udf_is_in()         |

```bash
gremlin> eq(2).test(3);
nebula> YIELD 3 == 2;

gremlin> within('a','b','c').test('d');
nebula> YIELD udf_is_in('d', 'a', 'b', 'c');
```

```bash
# Find pluto's co-habitants and exclude himself
gremlin> g.V(pluto).out('lives').in('lives').where(is(neq(pluto))).values('name');
nebula> GO FROM hash("pluto") OVER lives YIELD lives._dst AS place | GO FROM $-.place OVER lives REVERSELY WHERE \
$$.character.name != "pluto" YIELD $$.character.name AS cohabitants;
```

### Logical Operators

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Is | is()    | ==         |
Not | not()    | !=         |
And | and()    | AND         |
Or | or()    | OR         |

```bash
# Find age greater than or equal to 30
gremlin> g.V().values('age').is(gte(30));
nebula> LOOKUP ON character WHERE character.age >= 30 YIELD character.age;

# Find character with name pluto and age 4000
gremlin> g.V().has('name','pluto').and().has('age',4000);
nebula> LOOKUP ON character WHERE character.name == 'pluto' AND character.age == 4000;

# Logical not
gremlin> g.V().has('name','pluto').out('brother').not(values('name').is('neptune')).values('name');
nebula> LOOKUP ON character WHERE character.name == 'pluto' YIELD character.name AS name | \
GO FROM $-.VertexID OVER brother WHERE $$.character.name != 'neptune' YIELD $$.character.name;
```

### Patterns

TODO

<!-- LOOKUP ON character WHERE character.age >= 30 YIELD character.age AS name | \
    GO FROM $-.VertexID OVER relation YIELD $-.name, relation.name, $$.entity.name -->





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

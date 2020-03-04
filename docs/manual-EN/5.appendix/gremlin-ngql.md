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

### Graph Exploration

```bash
# Gremlin version
gremlin> Gremlin.version();
==>3.3.5

# Return all the vertices
gremlin> g.V();
==>v[1]
==>v[2]
...
nebula> # Coming soon

# Count all the vertices
gremlin> g.V().count();
==>12
nebula> # Coming soon

# Count the vertices and edges by label
gremlin> g.V().groupCount().by(label);
==>[character:9,location:3]
gremlin> g.E().groupCount().by(label);
==>[mother:1,lives:5,father:2,brother:6,battled:3,pet:1]
nebula> # Coming soon

# Return all edges
gremlin> g.E();
==>e[13][2-father->1]
==>e[14][2-lives->3]
...
nebula> # Coming soon

# Return vertices labels
gremlin> g.V().label().dedup();
==>character
==>location

nebula> SHOW TAGS;
==================
| ID | Name      |
==================
| 15 | character |
------------------
| 16 | location  |
------------------

# Return edge types
gremlin> g.E().label().dedup();
==>father
==>lives
...nebula> SHOW EDGES;
================
| ID | Name    |
================
| 17 | father  |
----------------
| 18 | brother |
----------------
...

# Return all vertices properties
gremlin> g.V().valueMap();
==>[name:[saturn],type:[titan],age:[10000]]
==>[name:[jupiter],type:[god],age:[5000]]
...
nebula> # Coming soon

# Return properties of vertices labeled character
gremlin> g.V().hasLabel('character').valueMap();
==>[name:[saturn],type:[titan],age:[10000]]
==>[name:[jupiter],type:[god],age:[5000]]
...
```

### Traversing Edges

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Out adjacent vertices to the vertex | out(\<label>)       | GO FROM \<vertex_id> OVER \<edge_type>  |
In adjacent vertices to the vertex | in(\<label>)    | GO FROM \<vertex_id> OVER \<edge_type> REVERSELY          |
Both adjacent vertices of the vertex      | both(\<label>)   | GO FROM \<vertex_id> OVER \<edge_type> BIDIRECT           |

```bash
# Find the out adjacent vertices of a vertex along an edge
gremlin> g.V(jupiter).out('brother');
==>v[8]
==>v[5]
nebula> GO FROM hash("jupiter") OVER brother;
========================
| brother._dst         |
========================
| 6761447489613431910  |
------------------------
| -5860788569139907963 |
------------------------

# Find the in adjacent vertices of a vertex along an edge
gremlin> g.V(jupiter).in('brother');
==>v[5]
==>v[8]
nebula> GO FROM hash("jupiter") OVER brother REVERSELY;
=======================
| brother._dst        |
=======================
| 4863977009196259577 |
-----------------------
| 4863977009196259577 |
-----------------------

# Find the both adjacent vertices of a vertex along an edge
gremlin> g.V(jupiter).both('brother');
==>v[8]
==>v[5]
==>v[5]
==>v[8]
nebula> GO FROM hash("jupiter") OVER brother BIDIRECT;
=======================
| brother._dst        |
=======================
| 6761447489613431910 |
------------------------
| -5860788569139907963|
| 4863977009196259577 |
-----------------------
| 4863977009196259577 |
-----------------------

# Two hops out traverse
gremlin> g.V(hercules).out('father').out('lives');
==>v[3]
nebula> GO FROM hash("hercules") OVER father YIELD father._dst AS id | \
GO FROM $-.id OVER lives;
========================
| lives._dst           |
========================
| -1121386748834253737 |
------------------------
```

### Has Filter Condition

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Filter vertex via identifier | hasId(\<vertex_id>)       | FETCH PROP ON \<vertex_id> |
Filter vertex or edge via label, key and value  | has(\<label>, \<key>, \<value>)    | LOOKUP \<tag> \| \<edge_type> WHERE \<expression>        |

```bash
# Filter vertex with ID saturn
gremlin> g.V().hasId(saturn);
==>v[1]
nebula> FETCH PROP ON * hash("saturn");
==========================================================================
| VertexID             | character.name | character.age | character.type |
==========================================================================
| -4316810810681305233 | saturn         | 10000         | titan          |
--------------------------------------------------------------------------

# Find for vertices with tag "character" and "name" attribute value "hercules"

gremlin> g.V().has('character','name','hercules').valueMap();
==>[name:[hercules],type:[demigod],age:[30]]
nebula> LOOKUP ON character WHERE character.name == 'hercules' YIELD character.name, character.age, character.type;
=========================================================================
| VertexID            | character.name | character.age | character.type |
=========================================================================
| 5976696804486077889 | hercules       | 30            | demigod        |
-------------------------------------------------------------------------
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
==>v[9]
==>v[10]
nebula> GO FROM hash('hercules') OVER battled | LIMIT 2;
=======================
| battled._dst        |
=======================
| 530133512982221454  |
-----------------------
| -695163537569412701 |
-----------------------

# Find the last record
gremlin> g.V().has('character','name','hercules').out('battled').values('name').tail(1);
==>cerberus
nebula> GO FROM hash('hercules') OVER battled YIELD $$.character.name AS name | ORDER BY name | LIMIT 1;
============
| name     |
============
| cerberus |
------------

# Skip the first record and return one record
gremlin> g.V().has('character','name','hercules').out('battled').values('name').skip(1).limit(1);
==>hydra
nebula> GO FROM hash('hercules') OVER battled YIELD $$.character.name AS name | ORDER BY name | LIMIT 1,1;
=========
| name  |
=========
| hydra |
---------
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
==>[v[8],v[12]]
==>[v[8],v[2]]
==>[v[8],v[5]]
==>[v[8],v[11]]

# Find the shortest path from vertex pluto to vertex jupiter
nebula> LOOKUP ON character WHERE character.name== "pluto" YIELD character.name AS name | \
    FIND SHORTEST PATH FROM $-.VertexID TO hash("jupiter") OVER *;
============================================================
| _path_              |
============================================================
| 6761447489613431910 <brother,0> 4863977009196259577
------------------------------------------------------------
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
==>v[12]
==>v[2]
==>v[5]
==>v[11]
nebula> LOOKUP ON character WHERE character.name== "pluto" YIELD character.name AS name | \
    GO FROM $-.VertexID OVER *;
================================================================================================================
| father._dst | brother._dst         | lives._dst           | mother._dst | pet._dst            | battled._dst |
================================================================================================================
| 0           | -5860788569139907963 | 0                    | 0           | 0                   | 0            |
----------------------------------------------------------------------------------------------------------------
| 0           | 4863977009196259577  | 0                    | 0           | 0                   | 0            |
----------------------------------------------------------------------------------------------------------------
| 0           | 0                    | -4331657707562925133 | 0           | 0                   | 0            |
----------------------------------------------------------------------------------------------------------------
| 0           | 0                    | 0                    | 0           | 4594048193862126013 | 0            |
----------------------------------------------------------------------------------------------------------------

# Find path between vertex hercules and vertex cerberus
# Stop traversing when the dest vertex is cerberus
gremlin> g.V().hasLabel('character').has('name','hercules').repeat(out()).until(has('name', 'cerberus')).path();
==>[v[6],v[11]]
==>[v[6],v[2],v[8],v[11]]
==>[v[6],v[2],v[5],v[8],v[11]]
...
nebula> # Coming soon

# Find path sourcing from vertex hercules
# And the dest vertex type is character
gremlin> g.V().hasLabel('character').has('name','hercules').repeat(out()).emit(hasLabel('character')).path();
==>[v[6],v[7]]
==>[v[6],v[2]]
==>[v[6],v[9]]
==>[v[6],v[10]]
...
nebula> # Coming soon

# Find shortest path between pluto and saturn over any edge
# And the deepest loop is 3
gremlin> g.V('pluto').repeat(out().simplePath()).until(hasId('saturn').and().loops().is(lte(3))).hasId('saturn').path();
nebula> FIND SHORTEST PATH FROM hash('pluto') TO hash('saturn') OVER * UPTO 3 STEPS;
=================================================================================================
| _path_              |
=================================================================================================
| 6761447489613431910 <brother,0> 4863977009196259577 <father,0> -4316810810681305233
-------------------------------------------------------------------------------------------------
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
==>[name:[jupiter],type:[god],age:[5000]]
==>[name:[neptune],type:[god],age:[4500]]
nebula> GO FROM hash('pluto') OVER brother YIELD $$.character.name AS Name, $$.character.age as Age | ORDER BY Age DESC;
==================
| Name    | Age  |
==================
| jupiter | 5000 |
------------------
| neptune | 4500 |
------------------
```

### Group By

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Group by items | group().by()    | GROUP BY         |
Remove repeated items | dedup() | DISTINCT |
Group by items and count | groupCount() | GROUP BY COUNT  |

**Note:** The GROUP BY function can only be applied in the YIELD clause.

```bash
# Group vertices by label then count
gremlin> g.V().group().by(label).by(count());
==>[character:9,location:3]
nebula> # Coming soon

# Find vertex jupiter's out adjacency vertices, group by name, then count
gremlin> g.V(jupiter).out().group().by('name').by(count());
==>[sky:1,saturn:1,neptune:1,pluto:1]
nebula> GO FROM hash('jupiter') OVER * YIELD $$.character.name AS Name, $$.character.age as Age, $$.location.name | \
GROUP BY $-.Name YIELD $-.Name, COUNT(*);
======================
| $-.Name | COUNT(*) |
======================
|         | 1        |
----------------------
| pluto   | 1        |
----------------------
| saturn  | 1        |
----------------------
| neptune | 1        |
----------------------

# Find the distinct dest vertices sourcing from vertex jupiter
gremlin> g.V(jupiter).out().hasLabel('character').dedup();
==>v[1]
==>v[8]
==>v[5]
nebula> GO FROM hash('jupiter') OVER * YIELD DISTINCT $$.character.name, $$.character.age, $$.location.name;
===========================================================
| $$.character.name | $$.character.age | $$.location.name |
===========================================================
| pluto             | 4000             |                  |
-----------------------------------------------------------
| neptune           | 4500             |                  |
-----------------------------------------------------------
| saturn            | 10000            |                  |
-----------------------------------------------------------
|                   | 0                | sky              |
-----------------------------------------------------------
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
==>false
nebula> YIELD 3 == 2;
==========
| (3==2) |
==========
| false  |
----------

gremlin> within('a','b','c').test('d');
==>false
nebula> YIELD udf_is_in('d', 'a', 'b', 'c');
======================
| udf_is_in(d,a,b,c) |
======================
| false              |
----------------------
```

```bash
# Find pluto's co-habitants and exclude himself
gremlin> g.V(pluto).out('lives').in('lives').where(is(neq(pluto))).values('name');
==>cerberus
nebula> GO FROM hash("pluto") OVER lives YIELD lives._dst AS place | GO FROM $-.place OVER lives REVERSELY WHERE \
$$.character.name != "pluto" YIELD $$.character.name AS cohabitants;
===============
| cohabitants |
===============
| cerberus    |
---------------
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
==>10000
==>5000
==>4500
==>30
==>45
==>4000
nebula> LOOKUP ON character WHERE character.age >= 30 YIELD character.age;
========================================
| VertexID             | character.age |
========================================
| -4316810810681305233 | 10000         |
---------------------------------------–
| 4863977009196259577  | 5000          |
---------------------------------------–
| -5860788569139907963 | 4500          |
---------------------------------------–
| 5976696804486077889  | 30            |
---------------------------------------–
| -6780323075177699500 | 45            |
---------------------------------------–
| 6761447489613431910  | 4000          |
---------------------------------------–

# Find character with name pluto and age 4000
gremlin> g.V().has('name','pluto').and().has('age',4000);
==>v[8]
nebula> LOOKUP ON character WHERE character.name == 'pluto' AND character.age == 4000;
=======================
| VertexID            |
=======================
| 6761447489613431910 |
-----------------------

# Logical not
gremlin> g.V().has('name','pluto').out('brother').not(values('name').is('neptune')).values('name');
==>jupiter
nebula> LOOKUP ON character WHERE character.name == 'pluto' YIELD character.name AS name | \
GO FROM $-.VertexID OVER brother WHERE $$.character.name != 'neptune' YIELD $$.character.name;
=====================
| $$.character.name |
=====================
| jupiter           |
---------------------
```

### Statistical Operations

Name               | Gremlin | nGQL           |
-----              |---------|   -----       |
Sum | sum()    | SUM()         |
Max | max()    | MAX()         |
Min | min()    | MIN()         |
Mean | mean()    | AVG()         |

> **Nebula Graph** statistical operations must be applied with `GROUP BY`.

```bash
# Calculate the sum of ages of all characters
gremlin> g.V().hasLabel('character').values('age').sum();
==>23595
nebula> # Coming soon

# Calculate the sum of the out brother edges of all characters
gremlin> g.V().hasLabel('character').map(outE('brother').count()).sum();
==>6
nebula> # Coming soon

# Return the max age of all characters
gremlin> g.V().hasLabel('character').values('age').max();
==>10000
nebula> # Coming soon
```

### Selecting and Filtering Paths

```bash
# Select the results of steps 1 and 3 from the path as the final result
gremlin> g.V(pluto).as('a').out().as('b').out().as('c').select('a','c');
==>[a:v[8],c:v[3]]
==>[a:v[8],c:v[1]]
...
nebula> # Coming soon

# Specify dimensions via by()
gremlin> g.V(pluto).as('a').out().as('b').out().as('c').select('a','c').by('name');
==>[a:pluto,c:sky]
==>[a:pluto,c:saturn]
...
nebula> # Coming soon

# Selects the specified key value from the map
gremlin> g.V().valueMap().select('name').dedup();
==>[saturn]
==>[jupiter]
...
nebula> # Coming soon
```

### Branches

```bash
# Traverse all vertices with label 'character'
# If name is 'jupiter', return the age property
# Else return the name property
gremlin> g.V().hasLabel('character').choose(values('name')).option('jupiter', values('age')).option(none, values('name'));
==>saturn
==>5000
==>neptune
...

# Lambda
gremlin> g.V().branch {it.get().value('name')}.option('jupiter', values('age')).option(none, values('name'));
==>saturn
==>5000
...

# Traversal
gremlin> g.V().branch(values('name')).option('jupiter', values('age')).option(none, values('name'));
==>saturn
==>5000

# Branch
gremlin> g.V().choose(has('name','jupiter'),values('age'),values('name'));
==>saturn
==>5000

# Group based on if then
gremlin> g.V().hasLabel("character").groupCount().by(values("age").choose(
           is(lt(40)),constant("young"),
            choose(is(lt(4500)),
                   constant("old"),
                  constant("very old"))));
==>[young:4,old:2,very old:3]
```

> Similar function is yet to be supported in **Nebula Graph**.

### Coalesce

The `coalesce()` step evaluates the provided traversals in order and returns the first traversal that emits at least one element.

The `optional()` step returns the result of the specified traversal if it yields a result else it returns the calling element, i.e. the identity().

The `union()` step supports the merging of the results of an arbitrary number of traversals.

```bash
# If type is monster, return type. Else return 'Not a monster'.
gremlin> g.V(pluto).coalesce(has('type','monster').values('type'),constant("Not a monster"));
==>Not a monster

# Find the following edges and adjacent vertices of jupiter in order, and stop when finding one
# 1. Edge brother out adjacent vertices
# 2. Edge father out adjacent vertices
# 3. Edge father in adjacent vertices
gremlin> g.V(jupiter).coalesce(outE('brother'), outE('father'), inE('father')).inV().path().by('name').by(label);
==>[jupiter,brother,pluto]
==>[jupiter,brother,neptune]

# Find pluto's father, if there is not any then return pluto himself
gremlin> g.V(pluto).optional(out('father')).valueMap();
==>[name:[pluto],type:[god],age:[4000]]

# Find pluto's father and brother, union the results then return the paths
gremlin> g.V(pluto).union(out('father'),both('brother')).path();
==>[v[8],v[2]]
==>[v[8],v[5]]
```

> Similar function is yet to be supported in **Nebula Graph**.

### Aggregating and Unfolding Results

```bash
# Collect results of the first step into set x
# Note: This operation doesn't affect subsequent results
gremlin> g.V(pluto).out().aggregate('x');
==>v[12]
==>v[2]
...

# Specify the aggregation dimensions via by ()
gremlin> g.V(pluto).out().aggregate('x').by('name').cap('x');
==>[tartarus,jupiter,neptune,cerberus]

# Find pluto's 2 hop out adjacent neighbors
# Collect the results in set x
# Show the neighbors' name
gremlin> g.V(pluto).out().aggregate('x').out().aggregate('x').cap('x').unfold().values('name');
==>tartarus
==>tartarus
...
```

> Similar function is yet to be supported in **Nebula Graph**.

### Matching Patterns

The `match()` step provides a more declarative form of graph querying based on the notion of pattern matching. With match(), the user provides a collection of "traversal fragments," called patterns, that have variables defined that must hold true throughout the duration of the match().

```bash
# Matching each vertex with the following pattern. If pattern is met, return map<String, Object>, els filter it.
# Pattern 1: a is jupiter's son
# Pattern 2: b is jupiter
# Pattern 3: c is jupiter's brother, whose age is 4000
gremlin> g.V().match(__.as('a').out('father').has('name', 'jupiter').as('b'), __.as('b').in('brother').has('age', 4000).as('c'));
==>[a:v[6],b:v[2],c:v[8]]

# match() can be applied with  select() to select partial results from Map <String, Object>
gremlin> g.V().match(__.as('a').out('father').has('name', 'jupiter').as('b'), __.as('b').in('brother').has('age', 4000).as('c')).select('a', 'c').by('name');
==>[a:hercules,c:pluto]

# match () can be applied with where () to filter the results
gremlin> g.V().match(__.as('a').out('father').has('name', 'jupiter').as('b'), __.as('b').in('brother').has('age', 4000).as('c')).where('a', neq('c')).select('a', 'c').by('name');
==>[a:hercules,c:pluto]
```

### Random filtering

The `sample()` step accepts an integer value and samples the maximum number of the specified results randomly from the previous traverser.

The `coin()` step can randomly filter out a traverser with the given probability. You give coin a value indicating how biased the toss should be.

```bash
# Randomly select 2 out edges from all vertices
gremlin> g.V().outE().sample(2);
==>e[15][2-brother->5]
==>e[18][5-brother->2]

# Pick 3 names randomly from all vertices
gremlin> g.V().values('name').sample(3);
==>hercules
==>sea
==>jupiter

# Pick 3 randomly from all characters based on age
gremlin> g.V().hasLabel('character').sample(3).by('age');
==>v[1]
==>v[2]
==>v[6]

# Applied with local to do random walk
# Starting from pluto, conduct random walk 3 times
gremlin> g.V(pluto).repeat(local(bothE().sample(1).otherV())).times(3).path();
==>[v[8],e[26][8-brother->5],v[5],e[18][5-brother->2],v[2],e[13][2-father->1],v[1]]

# Filter each vertex with a probability of 0.5
gremlin> g.V().coin(0.5);
==>v[1]
==>v[2]
...

# Return the name attribute of all vertices labeled location, otherwise return not a location
gremlin> g.V().choose(hasLabel('location'), values('name'), constant('not a location'));
==>not a location
==>not a location
==>sky
...
```

### Sack

A traverser that contains a local data structure is called a "sack". The `sack()` step is used to read and write sacks. Each sack of each traverser is created with `withSack()`.

```bash
# Defines a Gremlin sack with a value of one and return values in the sack
gremlin> g.withSack(1).V().sack();
==>1
==>1
...
```

### Barrier

The `barrier()` step turns the lazy traversal pipeline into a bulk-synchronous pipeline. It's useful when everything prior to barrier() needs to be executed before moving onto the steps after the barrier().

```bash
# Calculate the Eigenvector Centrality with barrier
# Including groupCount and cap, sorted in descending order
gremlin> g.V().repeat(both().groupCount('m')).times(5).cap('m').order(local).by(values, decr);
```

### Local

A GraphTraversal operates on a continuous stream of objects. In many situations, it is important to operate on a single element within that stream. To do such object-local traversal computations, `local()` step exists.

```bash
# Without local()
gremlin> g.V().hasLabel('character').as('character').properties('age').order().by(value,decr).limit(2).value().as('age').select('character', 'age').by('name').by();
==>[character:saturn,age:10000]
==>[character:jupiter,age:5000]

# With local()
gremlin> g.V().hasLabel('character').as('character').local(properties('age').order().by(value).limit(2)).value().as('age').select('character', 'age').by('name').by()
==>[character:saturn,age:10000]
==>[character:jupiter,age:5000]
==>[character:neptune,age:4500]
==>[character:hercules,age:30]
...

# Return the property map of monster
gremlin> g.V()hasLabel('character').has('type', 'type').propertyMap();
==>[name:[vp[name->nemean]],type:[vp[type->monster]],age:[vp[age->20]]]
==>[name:[vp[name->hydra]],type:[vp[type->monster]],age:[vp[age->0]]]
==>[name:[vp[name->cerberus]],type:[vp[type->monster]],age:[vp[age->0]]]

# Find number of monster
gremlin> g.V()hasLabel('character').has('type', 'monster').propertyMap().count(local);
==>3
==>3
==>3

# Find the max vertices number labeled tha same tag
gremlin> g.V().groupCount().by(label).select(values).max(local);
==>9

# List the first attribute of all vertices
gremlin> g.V().valueMap().limit(local, 1);
==>[name:[saturn]]
==>[name:[jupiter]]
==>[name:[sky]]
...

# Without local
gremlin> g.V().valueMap().limit(1);
==>[name:[saturn],type:[titan],age:[10000]]

# All vertices as a set, sample 2 from it
gremlin> g.V().fold().sample(local,2);
==>[v[8],v[1]]
```

### Statistics and Analysis

Gremlin provides two steps for statistics and analysis of the executed query statements:

- The `explain()` step will return a TraversalExplanation. A traversal explanation details how the traversal (prior to explain()) will be compiled given the registered traversal strategies.
- The `profile()` step allows developers to profile their traversals to determine statistical information like step runtime, counts, etc.

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

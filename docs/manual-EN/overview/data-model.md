# Graph Data Modeling
This guide is designed to walk you through the graph data modeling  of Nebula Graph. Basic concepts of designing a graph data model will be introduced.

## Directed Property Graph

The data model handled by the **Nebula Graph** is a **_directed property graph_**, whose edges are directional and there could be properties on both edges and vertices. It can be represented as:
**G = < V, E, P<sub>V</sub>, P<sub>E</sub> >**

Here **V** is a set of nodes aka vertices, **E** is a set of directional edges,
**P<sub>V</sub>** represents properties on vertices, and **P<sub>E</sub>** is the properties on edges.

## Tags
In **Nebula Graph**, vertex properties are clustered by **tags**. A tag is like an entity type, which contains multiple properties. Each vertex can have multiple tags associated. There could be dependencies between tags, too.

For instance, we could have a tag **Person**, which contains first_name, last_name, date_of_birth, etc. We could have another tag **Developer**, which has properties favorite_lang, github_account, etc. Tag **Developer** would depend on tag **Person**, which means if a vertex has tag **Developer** associated, it must have tag **Person** associated as well.

## Vertices
We will use the example graph below to introduce the basic concepts of the property graph:

![image](https://user-images.githubusercontent.com/42762957/61120012-96b25a80-a4ce-11e9-8460-067cac52a1e0.png)

Vertices are typically used to represent entities in the real world. In the preceding example, there are two kind of vertices labeled with tag **player** and **team**. A vertex must be labeled with at least one tag. Each tag may have a set of properties called schema.

Consider the above picture as example, there are two tags: **player** and **team**. Player has three properties: id (vid), Name(string), and Age(int); team has two properties: id (vid) and Name(string).

Like Mysql, **Nebula Graph** is a strong typed database. The property name and data type are determined before the data is written.

## Edges
Edges are used to connect vertices. Each edge usually represents a relationship or a behavior between two vertices. In **Nebula Graph**, an edge is always directional and of a specific edge type. However, from the source vertex to the target vertex, there could be multiple edges of
the same edge type.
These edges will be differentiated by their rankings. So, any
edge is uniquely identified by the tuple [src_vertex, dst_vertex, edge_type, ranking]. In
the preceding example, edges are **serve**
and **likeness**.

## Edge Type

Each edge is an instance of an edge type. Each edge type contains a collection of properties (name, type, etc.).

## Properties

Both tags and edge types can have properties, which are key/value pairs, and the definition of these properties (name, type, etc.) are called **Schema** in the **Nebula Graph**. 

Like Mysql, **Nebula Graph** is a strong typed database. The name and data type of the properties are determined before the data is written. Vertices can have properties indirectly via associated tags, and edges can have properties too.

In the preceding example, the schema of the tag **player** has three kind of properties:

- id (vid)
- Name (string)
- Age (int)

The schema of the edge **serve** has two kinds of properties:

- start_year (int)
- end_year (int)







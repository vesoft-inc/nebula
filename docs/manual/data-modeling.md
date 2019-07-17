# Modeling data for Nebula Graph
### Goals
This guide is designed to walk you through modeling data for Nebula Graph. It will
start by explaining the fundamental elements first and then move on to explore some recommended approaches.
### Labeled property graph model
**Nebula Graph** handles **Labeled property graph model**, and these are its main
features:
- A labeled property graph can be represented as **G = < V, E, P<sub>V</sub>, P<sub>E</sub> >**. Here **V** is a set of vertices, aka nodes, **E** is the set of directional edges, **P<sub>V</sub>** represents properties on vertices, and **P<sub>E</sub>** is the properties on edges.
- The type of vertex is called tag. A vertex must be labeled with at least one tag. Each tag may have a set of properties called schema.
- Edges in Nebula Graph always have an edgetype, a start (src)and an end vertex (dst), and a direction. Each edgetype defines the properties of the edge. Adding properties
to edges can provide additional metadata for graph algorithms, add additional semantics to relationships (including quality and weight), and constrain queries at runtime.

### Querying graphs: An instruction to nGQL
**nGQL** is a declarative, textual query language like SQL, but for graphs. Unlike SQL, nGQL is all about expressing graph patterns. nGQL is a work in progress. We will add more features and further simplify the existing ones. There might be inconsistency between the syntax specs and implementation for the time being.

This isn’t a reference document for nGQL, however—merely a friendly introduction so that we can explore more interesting graph query scenarios later on.
#### Main clauses
- CREATE

    Create spaces, tags and edges.
- INSERT

    Insert vertices and edges.
-     
- WHERE

    Provides criteria for filtering pattern matching results.
- YIELD

    Specifies which vertices, edges, and properties in the matched data should be returned to the client.
### How to start modeling for Nebula Graph

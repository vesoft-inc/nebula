# Data modeling and design concept
------
**Nebula Graph** is composed of four components: storage service, meta service, query engine and client. This document is to walk you through the graph data modeling of **Nebula Graph** and provide an architecture overview.

---
## Directed property graph

The data model handled by the **Nebula Graph** is a **_directed property graph_**, whose edges are directional and there could be properties on both edges and vertices. It can be represented as:

**G = < V, E, P<sub>V</sub>, P<sub>E</sub> >**

Here **V** is a set of nodes, aka vertices, **E** is a set of directional edges,
**P<sub>V</sub>** represents properties on vertices, and **P<sub>E</sub>** is the properties on edges.


![image](https://user-images.githubusercontent.com/42762957/61120012-96b25a80-a4ce-11e9-8460-067cac52a1e0.png)


<!-- ### Vertex

Vertices are typically used to represent entities in the real world. In the preceding example, vertices are **player** and **team**.

### Edges


Edges are used to connect vertices. Each edge usually represents a relationship (i.e ...) or A behavior (i.e. ...) between two vertices. In **Nebula Graph**, an edge is always directional and of a specific edge type. However, from the source vertex to the target vertex, there could be multiple edges of
the same edge type.
These edges will be differentiated by their rankings. So, any
edge is uniquely identified by the tuple [src_vertex, dst_vertex, edge_type, ranking]. In
the preceding example, edges are **serve**
and **likeness**.

### Tags

In **Nebula Graph**, vertex properties are clustered by **tags**. A tag is like an entity type, which contains multiple properties. Each vertex can have multiple tags associated. There could be dependency between tags, too.

For instance, we could have a tag **Person**, which contains first_name, last_name, date_of_birth, etc. We could have another tag **Developer**, which has properties favorite_lang, github_account, etc. Tag **Developer** would depend on tag **Person**, which means if a vertex has tag **Developer** associated, it must have tag **Person** associated as well.


### Properties

Both tags and edges can have properties, which are key/value pairs, and
the definition of these properties (name, type, etc.) are called **Schema** in the **Nebula Graph**. Like Mysql, **Nebula Graph** is a strong typed database. The name and data type of the properties are determined before the data is written. Vertices can have properties indirectly via associated tags, and edges can have properties too.

In the preceding example, the schema of the tag **player** has three kind of properties:

- id (vid)
- Name (string)
- Age (int)

 the schema of the edge **serve** has two kinds of properties:

 - start_year (int)
 - end_year (int)


### Edge type

Each edge is an instance of an edge type. Each edge type contains a collection of properties (name, type, etc.).

This part is written based on my own understand.
-->


### Vertex, vertex tag and tag properties

In **Nebula Graph**, the type of vertex is called **tag**. **A vertex must be labeled with at least one tag. Each tag may have a set of properties called schema.**

Consider the above picture as example, there are two tags: **player** and **team**. Player has three properties: id (vid), Name(string), and Age(int); team has two properties: id (vid) and Name(string).

Like Mysql, **Nebula Graph** is a strong typed database. The property name and data type are determined before the data is written.

### Edge, edge type and the corresponding edge properties

**Edges in Nebula Graph always have a type, a start (src) and an end vertex (dst), and a direction.** The direction is implicitly defined by the source and destination vertices. Edges can be self-referencing/looping.
<!-- but can never be dangling (missing a start or end vertex). -->
 Each edge in **Nebula Graph** is always of an edge type, has a source and a destination vertices, and carries a ranking number (optional).

In the preceding example, there are two kinds of edges, one is **_like_** pointing
from player to player, the property is likeness (double); the other is **_serve_**,
pointing from player to team, the properties are start_year(int) and end_year(int).

It should be noted that there may be **multiple edges of the same or different types at the same time** between any two vertices.



## Graph partition

![image](https://user-images.githubusercontent.com/42762957/61119934-76829b80-a4ce-11e9-9d49-2abb11b5f7b2.png)


In applications such as social networks, or business traction networks, the number of vertices and their connections are extremely large. Therefore, partitioning large graphs is inevitable for complexity reduction and parallelization. **Nebula Graph** adopts **Edge Separators** to cut a graph into smaller components. And the partition number cannot be changed once being configured.

## Data model


![image](https://user-images.githubusercontent.com/42762957/61120073-b0ec3880-a4ce-11e9-975f-c19482d4b109.png)



Data is stored as key-value pairs in **Nebula Graph**. For vertices, as illustrated in the diagram above, the key consists of four parts: partID (4 bytes), vertexID (8 bytes), tagID (4 bytes) and Version (8 bytes). And they are spread across various partitions based on the hash of their vertexIds (vid).


Each edge in **Nebula Graph** is stored as two keys. One is stored in the same partition as the source vertex, namely out-edge. The other one is stored in the same partition as the destination vertex, namely in-edge. Only the out-edge carries the properties.

The key format for the out-edge is: partID (4 bytes), sourceVertexID (8 bytes), edgeType (4 bytes), ranking (8 bytes), destinationVertexID (8 bytes)

The key format for the in-edge is: partID (4 bytes), destinationVertexID (8 bytes), -edgeType (4 bytes), ranking (8 bytes), sourceVertexID (8 bytes)


![image](https://user-images.githubusercontent.com/42762957/61120260-1c360a80-a4cf-11e9-8a43-8c4ca2d73572.png)



## Architecture

**Nebula Graph** includes four modules: storage service, meta service, query engine and client.


![image](https://user-images.githubusercontent.com/42762957/61120288-31ab3480-a4cf-11e9-9905-a1d4b1e6c523.png)




### Storage service

The corresponding process of storage service is nebula-storaged, which provides a key-value store. Separated storage and computing makes storage service flexible, and multiple storage engines like RocksDB and HBase are supported, with RocksDB set as default engine. To build a resilient distributed system, [Raft](https://raft.github.io/) is implemented as the consensus algorithm. Currently **Nebula Graph** supports
multiple storage engines like Rocksdb and HBase, etc.

Raft achieves data consensus via an elected leader. Based on that, nebula-storaged makes the following optimizations:

- Parallel Raft

      Partitions of the same ID from multiple machines form a raft group. And the parallel operations are implemented with multiple sets of Raft groups.

- Write Path & batch

      In Raft protocol, the master replicates log entries to all the followers and commits the entries in order. To improve the write throughput, **Nebula Graph** not only employs the parallel raft, but also implements the dynamic batch replication.

<!-- - Add Raft Learner

      When a new server joins the cluster, it can be added as a learner, which can neither vote nor count towards quorum. Once it catches up to the leader's logs, it can be promoted to follower as a normal voting node. -->

- Load-balance

      Migrating the partitions on an overworked server to other relatively idle servers to increases availability and capacity of the system.



![image](https://user-images.githubusercontent.com/42762957/61120371-6f0fc200-a4cf-11e9-8c41-9e531380205b.png)



### Meta service

The binary of the meta service is nebula-metad. Here is the list of its main functionalities:

-  User management

  In **Nebula Graph** different roles are assigned diverse privileges. We provide the following native roles: Global Admin, Graph Space Admin, User, Guest.

- Cluster configuration management

  Meta service manages the servers and partitions in the cluster, e.g. records location of the partitions, receives heartbeat from servers, etc. It balances the partitions and manages the communication traffic in case of server failure.

- Graph space management

  **Nebula Graph** supports multiple graph spaces. The data in different graph space are physically isolated. Meta service stores the metadata of all spaces in the cluster and tracks changes that take place in these spaces, like adding, dropping space, modifying graph space configuration (Raft copies).

-   Schema management

      **Nebula Graph** is a strong typed database.

      - Types of tag and edge properties are recorded by meta service. Supported data types are: int, double, timestamp, list, etc.

      - Multi-version management, supporting adding, modifying and deleting schema, and recording its version.

      - TTL (time-to-live) management, supporting automatic data deletion and space reclamation.

The meta service is stateful, and just like the storage service, it persists data to a key-value store.


![image](https://user-images.githubusercontent.com/42762957/61120413-8cdd2700-a4cf-11e9-8846-14b5d8bd6693.png)


### Query Engine & Query Language (nGQL)

The binary of the query engine is **nebula-graphd**. Each nebula-graphd instance is stateless and never talks to other nebula-graphd. nebula-graphd only talks to the storage service and the meta service. That makes it trivial to expand or shrink the query engine cluster.

The query engine accepts the message from the client and generates the execution plan after the lexical parsing (Lexer), semantic analysis (Parser) and the query optimization. Then the execution plan will be passed to the execution engine. The query execution engine takes the query plans and interacts with meta server and the storage engine to retrieve the schema and data.

The main optimizations of the query engine are:

- Asynchronous and parallel execution

      I/O operations and network transmission are time-consuming. Thus asynchronous and parallel operations are widely adopted in the query engine to reduce the latency and to improve the overall throughput.Also, a separate resource pool is set for each query to avoid the long tail effect of those time-consuming queries.

- Pushing down computation

      In a distributed system, transferring large amount of data on the network really hurts the overall latency. In **Nebula Graph**, the query engine will make decisions to push some filter and aggregation down to the storage service. The purpose is to reduce the amount of data passing back from the storage.

<!-- - Query optimizer

      Query optimizer for SQL has experienced a long-term development, but not for graph database. **Nebula Graph** has made an effort in the optimizations of graph query, including cache executing plan and executing context-free queries in parallel. -->


![image](https://user-images.githubusercontent.com/42762957/61119795-26a3d480-a4ce-11e9-97e9-102bf14e72d8.png)




### API and SDK

**Nebula Graph** provides SDKs in C++, Java, and Golang. **Nebula Graph** uses fbthrift as the RPC framework to communicate among servers. **Nebula Graph**'s web SDK is in progress and will be released soon.

# Data modeling and design concept
------
**Nebula Graph** is composed of four components: storage service, metadata service, query engine and client. This document is to walk you through the graph data modeling of Nebula Graph and depict an architecture overview.

---
## Directed property graph

**Nebula Graph** handles the **Directed Property Graph Model**, whose edge is directional
and both edges and vertices can have properties. It can be represented as

**G = < V, E, P<sub>V</sub>, P<sub>E</sub> >**

Here **V** is a set of nodes, aka vertices, **E** is the set of directional edges,
**P<sub>V</sub>** represents properties on vertices, and **P<sub>E</sub>** is the properties on edges.


![image](https://user-images.githubusercontent.com/42762957/61120012-96b25a80-a4ce-11e9-8460-067cac52a1e0.png)


<!-- ### Vertex

Vertices are typically used to store entities with properties. In the preceding example, vertices are **player** and **team**.

### Edges


Edges are used to connect vertices. They usually represent a relationship (i.e. ownership, friendship, and so on) or a behavior (i.e. transfer, like, and so on) between two vertices. In **Nebula Graph**, an edge is always directional and of a specific edge type. However, from the source vertex to the target vertex, there could be multiple edges of
the same edge type.
These edges will be differentiated by their rankings. So, any
edge is uniquely identified by the tuple [src_vertex, dst_vertex, edge_type, ranking]. In
the preceding example, edges are **serve**
and **likeness**.

### Tags

In Nebula Graph, the type of vertex is called a tag. A vertex must have at least
one tag, and Nebula allows you to set multiple tags per vertex.


### Properties

Both vertices and edges can have properties, which are key/value pairs, and
the definition of these properties (name, type, etc.) are **Schema** in the **Nebula Graph**. Like Mysql, Nebula Graph is a strong typed database. The name and data type of the properties are determined before the data is written. Vertex can have one or more properties, and edges can have properties too.

In the preceding example, the schema of **player** has three kind of properties:

- id (vid)
- Name (string)
- Age (int)

 the schema of **serve** has two kinds of properties:

 - start_year (int)
 - end_year (int)


### Edgetype

Each edge has an edgetype, and each edgetype defines the properties of the edge.

This part is written based on my own understand.
-->


### Vertex, type of vertex (tag) and the corresponding vertex properties

In Nebula Graph, the type of vertex is called **tag**. **A vertex must be labeled with at least one tag. Each tag may have a set of properties called schema.**

Consider the above picture as example, there are two tags: **player** and **team**. Player has three properties: id (vid), Name(string), and Age(int); team has two properties: id (vid) and Name(string).

Like Mysql, Nebula Graph is a strong schema database. The property name and data type are determined before the data is written.

### Edge, edgetype and the corresponding edge properties

**Edges in Nebula Graph always have a type, a start (src)and an end vertex (dst), and a direction.** They can be self-referencing/looping but can never be dangling (missing a start or end node). **Each edge has an edgetype, and each edgetype defines the properties of the edge.**

In the preceding example, there are two kinds of edges, one is **like** pointing
from player to player, the property is likeness (double); the other is **serve**,
pointing from team to serve, the properties are start_year(int) and end_year(int).

It should be noted that there may be **multiple edges of the same or different types
at the same time** between the start (src)and end vertex (dst).



## Graph partition

![image](https://user-images.githubusercontent.com/42762957/61119934-76829b80-a4ce-11e9-9d49-2abb11b5f7b2.png)


In applications such as social networks, or road networks, the number of vertices and their connections are extremely large. Therefore, partitioning large graphs is inevitable for complexity reduction and parallelization. Nebula Graph adopts **Edge Separators** to cut a graph into smaller components. And the partition number cannot be changed once being configured.

## Data model


![image](https://user-images.githubusercontent.com/42762957/61120073-b0ec3880-a4ce-11e9-975f-c19482d4b109.png)



Data is stored as key-value pairs in Nebula Graph. For vertices, as illustrated in the diagram above, the key consists of four parts: partID (4 bit), vertexID (8 bit), tagID (4 bit) and Version (8 bit). And they are spread across various partitions based on the hash of their vertexIds (vid).

For edges, the key consists of two separate key-values, i.e. out-key and
in-key, respectively. The out-key and the edge's corresponding starting point are stored in one partition, while the in-key and end point are stored in another one.



![image](https://user-images.githubusercontent.com/42762957/61120260-1c360a80-a4cf-11e9-8a43-8c4ca2d73572.png)



## Architecture

Nebula Graph includes four modules: storage service, metadata service, query engine and client.


![image](https://user-images.githubusercontent.com/42762957/61120288-31ab3480-a4cf-11e9-9905-a1d4b1e6c523.png)




### Storage serve

The corresponding process of storage service is nebula-storaged, and provides a key-value store. Separated storage and computing makes storage service flexible, and multiple storage engines like RocksDB and HBase are supported, with RocksDB set as default engine. To build a resilient distributed system, [Raft](https://raft.github.io/) is implemented as the consensus algorithm.

Currently it supports
multiple storage engines like Rocksdb and HBase, etc.

Raft achieves data consensus via an elected leader. Based on that, Nebula Storage makes the following optimizations:

- Parallel Raft

      Partitions of same ID from multiple machines form a raft group. And the concurrent operations are implemented with multiple sets of Raft groups.

- Write Path & batch

      In Raft protocol, multi-machine synchronization depends on log ID orders, which
  lead to low throughput. Nebula Graph achieves high throughput by batch and out-of-order commit.

- Add Raft Learner

      When a new server joins the cluster, it can be added as a learner node. The learner node can neither vote nor count towards quorum. Once it catches up to the leader's logs, it can be promoted to follower as a normal voting node.

- Load-balance

      Migrating the partitions on an overworked server to other relatively idle servers to increases availability and capacity of the system.



![image](https://user-images.githubusercontent.com/42762957/61120371-6f0fc200-a4cf-11e9-8c41-9e531380205b.png)



### Metadata service

The process corresponding to the Meta service is nebula-metad, and its main functions are:

-  User management

      In Nebula Graph different roles are assigned diverse privileges. We provide the following native roles: God user，Admin，Use and Guest.

- Cluster configuration management

      Metadata service manages the servers and partitions in the cluster, e.g. records location of the partitions, receives Heartbeat from servers, etc. It balances the partitions and manages the communication traffic in case of server failure.

- Graph space management

      Metadata service stores the metadata of all spaces in the cluster and tracks changes that take place in these spaces, like adding, dropping space, modifying graph space configuration (Raft copies).

-   Schema management

      Nebula Graph is a strong typed database.

      - Types of Tag and Edge properties are recorded by Meta service. Supported types are: int, double, timestamp, list, etc.

      - Multi-version management, supporting adding, modifying and deleting schema, and recording its version.

      - TTL (time-to-live) management, supporting automatic data deletion and space reclamation.

The Meta Service is stateful, and just like the Storage service, it it persists data to a key-value store.


![image](https://user-images.githubusercontent.com/42762957/61120413-8cdd2700-a4cf-11e9-8846-14b5d8bd6693.png)


### Query Engine & Query Language (nGQL)

The process corresponding to the query engine is nebula-graphd, which consists
of nodes that are stateless, equally privileged and isolated from each other.

The main function of the Query Engine layer is to analysis the nGQL text sent by the client and generate an execution plan through lexical analysis (Lexer) and parsing (Parser). Then the execution plan will be passed to the the execution engine. The query execution engine takes the query plans and interacts with meta server and the storage engine to retrieve the schema and data.

The main optimizations of the Query Engine layer are:

- Asynchronous and parallel execution

      I/O operations and network transmission are time-consuming. Thus asynchronous and parallel operations are adopted in Nebula Query Engine to guarantee quality of service (QoS).Also, a separate resource pool is set for each query to avoid the long tail effect of those time-consuming queries.

- Pushing down computation

      Too much data passing back form storage layer to computation layer occupies  too much bandwidth, to avoid that, condition filtering such as 'WHERE' clause is pushed down to the storage layer along with the conditions.

- Query optimizer

      Query optimizer for SQL has experienced a long-term development, but not for graph database. Nebula Graph makes an effort in the optimizations of graph query, including cache executing plan and execute context-free queries in parallel.


![image](https://user-images.githubusercontent.com/42762957/61119795-26a3d480-a4ce-11e9-97e9-102bf14e72d8.png)




### API and SDK

Nebula Graph provides SDKs in C++, Java, and Golang. Nebula Graph uses RPC to communicate among servers, and the communication protocol is Facebook-Thrift. Users can also use Nebula Graph on Linux. Nebula Graph's web SDK is in progress and will be released soon.

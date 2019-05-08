## Nebula Graph

High-performance, Distributed, Open source Graph Database.

Nebula is a horizontally scalable and distributed graph database, providing ACID transactions, consistent replication and linearizable reads. Nebula uses nGQL, a declarative, textual query language like SQL.

Nebula's goal is to provide reading, writing, and computing with high concurrency, low latency for super large scale networks. Nebula is an open source project and we are looking forward to working with the community to popularize and promote the graph database.

## Challenges in graph database

- Massive related data
- Distributed stored ACID
- High concurrency, low latency for on-line application
- Complex business logic

## Nebula features

- Support super large scale networks
 - billions of vertexes, trillions of edges, total data amount > 100TB
- High concurrency, low latency
 - single vertex can support up to 100,000 QPS, 500,000 TPS
 - the average latency of 2-hop is less than 10ms
- Storage, calculation separation
 - take full use of resources
- Improve computational delivery and reduce data transfer
 - reduce data locality, improve performance

## Why Nebula?

- Support multiple graph space
 - physical data isolation in different graph spaces
 - support different users
- Strong data consistency
 - adopted a majority quorum coherence protocol (copies can be set by users according to business needs)
- Support data's ACID features
- Support index of vertexes and edges
- Support massive data update
 - update volume > 10TB/day

## High Availability
- Computing node is stateless, load balancing automatically
- Data stored in multiple copies, fault tolerance is determined by the number of copies
- Computing and storage nodes can be expanded or contracted independently, and both support online expansion and contraction
- Support data synchronization among different data clusters(Synchronous mode and asynchronous mode)

## Install with Docker

 If you are using docker, you can use the official Nebula image.
 ```
docker pull vesoft/nebula-graph
 ```
**NOTE:** cmd install and source install are omitted
 ## Get Started
 To get started with Nebula, follow:
 - `Get Start` file in doc.
 - Tutorial and presentation videos on `youtube channel`.


 **Note：** `Get Start` and `youtube channel` are not ready yet.

 ## Is Nebula the right choice for me?
- Do you have more than 10 SQL tables, connected to each other via foreign keys?
- Do you have sparse data, which doesn't elegantly fit into SQL tables?
- Do you want a simple and flexible schema, which is readable and maintainable over time?
- Do you care about speed and performance at scale?

If the answers to the above are YES, then Nebula would be a great fit for your application. Nebula provides NoSQL like scalability while providing SQL like transactions and ability to select, filter and aggregate data points. It combines that with distributed joins, traversals and graph operations, which makes it easy to build applications with it.

## Nebula compared to other DBs

|    | Nebula Graph | Dgraph | TigerGraph | JanusGraph | Neo4j |
| --- | ----         | ---    | ----       | ---------- | ----- |
| Goal | General Online Graph DB | Knowledge Graph DB | General Online Graph DB | General Offline Graph DB | General Online Graph DB |
| Perf. (2-hop) | < 10ms | 10 - 20ms | 10 - 20ms | ~100ms | < 10ms |
| Perf. (2-hop with filters) | ~10ms | >20ms | >20ms | > 100ms | < 20ms |
| Arch. | Symmetrically distributed | Symmetrically distributed | Asymmetrically distributed | External | Nondistributed |
| Storage | RocksDB | Badger | In-house? | Hbase / Cassandra | In-memory / Disk persistence |
| Query Lang. | SQL-like | GraphQL (XML based) | Functional GQL | Gremlin | Cyther|
| ACID | Yes | Yes | No? | Yes | No |


## Users
- ** Nebula official documentation is present at** *** our official website.
- Check out the demo at ***.
- Read about latest updates from Nebula team on our blog.
- Watch tech talks on our Youtube channel.

## Developers
- See a list of issues we need help with.
- Please see how-to-contribute.md for guidelines on contributions.

## Contact
- Please use *** website for documentation, questions, feature requests and discussions.
- Please use Github issue tracker for filling bugs or feature requests.
- Follow us on twitter @Nebula.

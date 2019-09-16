# Nebula Graph User Manual

### About this manual

This is the Nebula Graph User Manual. It documents Nebula Graph R201910_beta. For information about which versions have been released, see  Nebula Graph Release Notes.
<!-- Release Notes not added yet. -->

This manual presumes you understand general graph database concepts and command-line interpreter.

Nebula Graph is under constant development, and the User Manual is updated frequently as well.

The User Manual source files are written in  Markdown format. The HTML version is produced automatically, primarily using mkdocs. For information about mkdocs, see https://www.mkdocs.org/

If you have questions about using Nebula Graph, join the [Nebula Graph Community Slack](https://join.slack.com/t/nebulagraph/shared_invite/enQtNjIzMjQ5MzE2OTQ2LTM0MjY0MWFlODg3ZTNjMjg3YWU5ZGY2NDM5MDhmOGU2OWI5ZWZjZDUwNTExMGIxZTk2ZmQxY2Q2MzM1OWJhMmY#"). If you have suggestions concerning additions or corrections to the manual itself, please do not hesitate to open an issue on [GitHub](https://github.com/vesoft-inc/nebula/issues).

This manual was originally written and maintained by the Nebula Graph Documentation Team.

### Typographical and syntax conventions
This manual uses certain typographical conventions:

- `Fixed width`

    A fixed-width font is used in syntax, code examples, system output, and file names.

- **Bold**

    Bold typeface indicates commands or characters the user types, provides emphasis, or the names of user interface elements.

- *Italic*

    Italic typeface indicates the title of a document, or signifies new terms.
    
- `UPPERCASE fixed width`

     Keywords in syntax and code examples are almost always shown in upper case. Although shown in uppercase, you can type keywords in either uppercase or lowercase.

<!-- ### Overview of Nebula Graph
#### What is Nebula Graph?
**Nebula Graph** is an open source (Apache 2.0 licensed), distributed, scalable, lightning-fast graph database. It is the only solution in the world capable to host graphs with dozens of billions of vertices (nodes) and trillions of edges, while still provides millisecond latency.

**Nebula Graph's** goal is to provide reading, writing, and computing with high concurrency, low latency for super large scale graphs. Nebula Graph is an open source project and we are looking forward to working with the community to popularize and promote the graph database.
#### The main features of Nebula Graph
This section describes some of the important characteristics of Nebula Graph.
- High performance

    Nebula Graph provides low latency read and write, while still maintaining high throughput.
- SQL-like

    Nebula Graph's SQL-like query language is easy to understand and powerful enough to meet complex business needs.
- Secure

    With role-based access control, Nebula Graph only allows authenticated access.
- Scalable

    With shared-nothing distributed architecture, Nebula Graph offers linear scalability.
- Extensible

    Nebula Graph supports multiple storage engine types. The query language can be extended to support new algorithms.
- Transactional

    With distributed ACID transaction support, Nebula Graph ensures data integrity.
- Highly available

    Nebula Graph guarantees high availability even in case of failures. -->

<!-- ### Architectural overview
**Nebula Graph** is composed of four components: storage service, metadata service, query engine and client. Nebula Graph's modular architecture allows it to store super large scale graphs，query complex business logic and maintain data persistency.

Storage service is flexible, load-balanced and supports multiple storage engines like RocksDB and HBase.  

Metadata service provides user management, cluster configuration management, graph space management and schema management.

Query engine analyses the nGQL text sent by the client and generate an execution plan. The query execution engine takes the query plans and interacts with meta server and the storage engine to retrieve the schema and data.

Nebula Graph provides SDKs in C++, Java, and Golang. Nebula Graph uses RPC to communicate among servers, and the communication protocol is Facebook-Thrift.

**Figure 1 High-performance Nebula Graph architecture**

![image](https://user-images.githubusercontent.com/42762957/61120288-31ab3480-a4cf-11e9-9905-a1d4b1e6c523.png) -->

# Welcome to the Official Nebula Graph Documentation

**Nebula Graph** is a distributed, scalable, and lightning-fast graph database.

It is the optimal solution in the world capable of hosting graphs with dozens of billions of vertices (nodes) and trillions of edges with millisecond latency.

## Prefix

* [About This Manual](0.about-this-manual.md)
* [Manual Change Log](CHANGELOG.md)

## Overview (for Beginners)

* [Introduction](1.overview/0.introduction.md)
* Concepts
  * [Data Model](1.overview/1.concepts/1.data-model.md)
  * [Query Language Overview](1.overview/1.concepts/2.nGQL-overview.md)
* Quick Start and Useful Links
  * [Get Started](1.overview/2.quick-start/1.get-started.md)
  * [Trouble Shooting](1.overview/2.quick-start/2.trouble-shooting.md)
  * [Build Source Code](3.build-develop-and-administration/1.build/1.build-source-code.md)
  * [Deploy Cluster](3.build-develop-and-administration/3.deploy-and-administrations/deployment/deploy-cluster.md)
  * [Import .csv File](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/import-csv-file.md)
  * [Ingest .sst File](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/download-and-ingest-sst-file.md)
  * [Nebula Graph Clients](1.overview/2.quick-start/3.supported-clients.md)
  * [FAQ](1.overview/2.quick-start/2.FAQ.md)


* Design and Architecture
  * [Design Overview](1.overview/3.design-and-architecture/1.design-and-architecture.md)
  * [Storage Architecture](1.overview/3.design-and-architecture/2.storage-design.md)
  * [Query Engine](1.overview/3.design-and-architecture/3.query-engine.md)

## Query Language (for All Users)

* Data Types
  * [Data Types](2.query-language/1.data-types/data-types.md)
  * [Type Conversion](2.query-language/1.data-types/type-conversion.md)
* Functions and Operators
  * [Bitwise Operators](2.query-language/2.functions-and-operators/bitwise-operators.md)
  * [Built-In Functions](2.query-language/2.functions-and-operators/built-in-functions.md)
  * [Comparison Functions And Operators](2.query-language/2.functions-and-operators/comparison-functions-and-operators.md)
  * [Group By Function](2.query-language/2.functions-and-operators/group-by-function.md)
  * [Limit Syntax](2.query-language/2.functions-and-operators/limit-syntax.md)
  * [Logical Operators](2.query-language/2.functions-and-operators/logical-operators.md)
  * [Order By Function](2.query-language/2.functions-and-operators/order-by-function.md)
  * [Set Operations](2.query-language/2.functions-and-operators/set-operations.md)
  * [uuid Function](2.query-language/2.functions-and-operators/uuid.md)
* Language Structure
  * Literal Values
    * [Boolean Literals](2.query-language/3.language-structure/literal-values/boolean-literals.md)
    * [Numeric Literals](2.query-language/3.language-structure/literal-values/numeric-literals.md)
    * [String Literals](2.query-language/3.language-structure/literal-values/string-literals.md)
  * [Comment Syntax](2.query-language/3.language-structure/comment-syntax.md)
  * [Identifier Case Sensitivity](2.query-language/3.language-structure/identifier-case-sensitivity.md)
  * [Pipe Syntax](2.query-language/3.language-structure/pipe-syntax.md)
  * [Property Reference](2.query-language/3.language-structure/property-reference.md)
  * [Schema Object Names](2.query-language/3.language-structure/schema-object-names.md)
  * [Statement Composition](2.query-language/3.language-structure/statement-composition.md)
* Statement Syntax
  * Data Definition Statements
    * [Alter Tag/Edge Syntax](2.query-language/4.statement-syntax/1.data-definition-statements/alter-tag-edge-syntax.md)
    * [Create Space Syntax](2.query-language/4.statement-syntax/1.data-definition-statements/create-space-syntax.md)
    * [Create Tag/Edge Syntax](2.query-language/4.statement-syntax/1.data-definition-statements/create-tag-edge-syntax.md)
    * [Drop Edge Syntax](2.query-language/4.statement-syntax/1.data-definition-statements/drop-edge-syntax.md)
    * [Drop Space Syntax](2.query-language/4.statement-syntax/1.data-definition-statements/drop-space-syntax.md)
    * [Drop Tag Syntax](2.query-language/4.statement-syntax/1.data-definition-statements/drop-tag-syntax.md)
  * Data Query and Manipulation Statements
    * [Delete Edge Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/delete-edge-syntax.md)
    * [Delete Vertex Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/delete-vertex-syntax.md)
    * [Fetch Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/fetch-syntax.md)
    * [Go Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/go-syntax.md)
    * [Insert Edge Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/insert-edge-syntax.md)
    * [Insert Vertex Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/insert-vertex-syntax.md)
    * [Update Vertex/Edge Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/update-vertex-edge-syntax.md)
    * [Where Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/where-syntax.md)
    * [Yield Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/yield-syntax.md)
    * [Return Syntax](2.query-language/4.statement-syntax/2.data-query-and-manipulation-statements/return-syntax.md)
  * Utility Statements
    * [Describe Syntax](2.query-language/4.statement-syntax/3.utility-statements/describe-syntax.md)
    * [Use Syntax](2.query-language/4.statement-syntax/3.utility-statements/use-syntax.md)
    * [Show Syntax](2.query-language/4.statement-syntax/3.utility-statements/show-syntax.md)
  * Graph Algorithms
    * [Find Path Syntax](2.query-language/4.statement-syntax/4.graph-algorithms/find-path-syntax.md)

## Build Develop And Administration (For Developers And DBA)

* Build
  * [Build Source Code](3.build-develop-and-administration/1.build/1.build-source-code.md)
  * [Build By Docker](3.build-develop-and-administration/1.build/2.build-by-docker.md)
* Develop and Interface
  * [Key Value API](3.build-develop-and-administration/2.develop-and-interface/kv-interfaces.md)
  * [Nebula Graph Clients](1.overview/2.quick-start/3.supported-clients.md)

* Deploy and Administrations
  * Deployment
    * [Configuration Description](3.build-develop-and-administration/3.deploy-and-administrations/deployment/configuration-description.md)
    * [Deploy Cluster On Docker](3.build-develop-and-administration/3.deploy-and-administrations/deployment/deploy-cluster-on-docker.md)
    * [Deploy Cluster](3.build-develop-and-administration/3.deploy-and-administrations/deployment/deploy-cluster.md)
    * [Connect Prometheus](3.build-develop-and-administration/3.deploy-and-administrations/deployment/connect-prometheus.md)

  * Server Administration
    * Account Management Statements
      * [Drop User Syntax](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/account-management-statements/drop-user-syntax.md)
    * Configuration Statements
      * [Configs Syntax](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/configs-syntax.md)
      * [RocksDB Compaction and Flush](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/rocksdb-compaction-flush.md)
      * [Logs](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/configuration-statements/log.md)
    * Graph Service Administration
      * [Graph Metrics](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/graph-service-administration/graph-metrics.md)
    * Meta Service Administration
      * [Meta Metrics](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/meta-service-administration/meta-metrics.md)
    * Storage Service Administration
      * Data Import
        * [Download And Ingest .sst File](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/download-and-ingest-sst-file.md)
        * [Import .csv File](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/import-csv-file.md)
        * [Spark Writer](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/data-import/spark-writer.md)
      * [Storage Balance](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/storage-balance.md)
      * [Storage Metrics](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/storage-metrics.md)
      * [Cluster Snapshot](3.build-develop-and-administration/3.deploy-and-administrations/server-administration/storage-service-administration/cluster-snapshot.md)

## Contributions (for Contributors)

* [Contribute to Documentations](4.contributions/contribute-to-documentation.md)
* [Cpp Coding Style](4.contributions/cpp-coding-style.md)
* [Developer Documentation Style Guide](4.contributions/developer-documentation-style-guide.md)
* [How to Contribute](4.contributions/how-to-contribute.md)

## Misc

### Video

* [YouTube](https://www.youtube.com/channel/UC73V8q795eSEMxDX4Pvdwmw/)
* [bilibili](https://space.bilibili.com/472621355)

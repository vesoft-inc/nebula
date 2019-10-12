# Welcome to the official Nebula Graph documentation.
---

**Nebula Graph** is a distributed, scalable, lightning-fast graph database. It is the only solution in the world capable to host graphs with dozens of billions of vertices (nodes) and trillions of edges, while still provides millisecond latency.

<!--
**Nebula Graph's** goal is to provide reading, writing, and computing with high concurrency, low latency for super large scale graphs. Nebula is an open source project and we are looking forward to working with the community to popularize and promote the graph database.

**Nebula Graph's** primary features

 * Symmetrically distributed
 * Highly scalable
 * Fault tolerant
 * SQL-like query language
-->
---

## Overview
---

* [**[Concepts](https://github.com/vesoft-inc/nebula/blob/master/README.md)**] An overall concept of Nebula Graph

* [**[Getting Started](https://github.com/vesoft-inc/nebula/blob/master/docs/get-started.md)**] Tutorial to help start with Nebula Graph
* [**[Query Language nGQL](https://github.com/vesoft-inc/nebula/blob/master/docs/nGQL-tutorial.md)**] Tutorial of Nebula Graph's query language nGQL



## User Manual
---

* Data Types

  * [Numeric Types](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/data-types/numeric-types.md)
  * [String Types](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/data-types/string-types.md)


* Function and Operators

  * [Comparison Functions and Operators](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/comparison-functions-and-operators.md)
  * [Functions and Operator Reference](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/functions-and-operator-reference.md)
  * [Group by Function](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/group-by-function.md)
  * [Logical Operators](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/logical-operators.md)
  * [Operator Precedence](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/operator-precedence.md)
  * [Order by Function](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/order-by-function.md)
  * [Set Operations](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/set-operations.md)
  * [Type Conversion](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/type-conversion.md)


* Language Structure
  * Literal Values

    * [Null Values](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/literal-values/NULL-values.md)
    * [Boolean Literals](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/literal-values/boolean-literals.md)
    * [Numeric Literals](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/literal-values/numeric-literals.md)
    * [String Literals](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/literal-values/string-literals.md)

  <!-- * [Comment syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/comment-syntax.md) -->
  * [Comment Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/comment-syntax.md)
  * [Expression](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/expression.md)
  * [Identifier Case Sensitivity](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/identifier-case-sensitivity.md)
  * [Keywords and Reserved Words](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/keywords-and-reserved-words.md)
  * [Property Reference](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/property-reference.md)
  * [Schema Object Names](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/schema-object-names.md)
  * [Statement Composition](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/statement-composition.md)
  * [User Defined Variable](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/user-defined_variable.md)


* Statement Syntax
  * Data Administration Statements

    * Account Management Statements
      * [Drop User Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-administration-statements/account-management-statements/drop-user-syntax.md)
    * Configuration Statements
      * [Variables Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-administration-statements/configuration-statements/variables-syntax.md)
  * Data Definition Syntax
    * [Alter Tag Edge Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-definition-statements/alter-tag-edge-syntax.md)
    * [Create Space Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-definition-statements/create-space-syntax.md)
    * [Create Tag Edge Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-definition-statements/create-tag-edge-syntax.md)
    * [Drop Edge Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-definition-statements/drop-edge-syntax.md)
    * [Drop Space Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-definition-statements/drop-space-syntax.md)
    * [Drop Tag Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-definition-statements/drop-tag-syntax.md)
    * [Show Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-definition-statements/show-syntax.md)

  * Data Manipulation Statements

    * [Fetch Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-manipulation-statements/fetch-syntax.md)
    * [Go Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-manipulation-statements/go-syntax.md)
    * [Insert Edge Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-manipulation-statements/insert-edge-syntax.md)
    * [Insert Vertex Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-manipulation-statements/insert-vertex-syntax.md)
    * [Pipe Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-manipulation-statements/pipe-syntax.md)
    * [Where Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-manipulation-statements/where-syntax.md)
    * [Yield Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/data-manipulation-statements/yield-syntax.md)

  * Utility Statements
    * [Describe Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/utility-statements/describe-syntax.md)
    * [Use Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/statement-syntax/utility-statements/use-syntax.md)

* Build and Deployment
    * [**[Build Nebula Graph](https://github.com/vesoft-inc/nebula/blob/master/docs/how-to-build.md)**] Introductions on how to build source code
    * [**[Deploy Clustter](https://github.com/vesoft-inc/nebula/blob/master/docs/deploy-cluster.md)**]
    How to deploy cluster
* Misc
    * [**[Contribute to Nebula Graph](https://github.com/vesoft-inc/nebula/blob/master/docs/how-to-contribute.md)**] Introductions on how to contribute to Nebula Graph
    * [**[Contribute to Documentation](https://github.com/vesoft-inc/nebula/blob/master/docs/contribute-to-documentation.md)**] Contribute to document 
    * [**[Style Guild](https://github.com/vesoft-inc/nebula/blob/master/docs/developer-documentation-style-guide.md)**] Develop style guild

## Videos

   * [YouTube](https://www.youtube.com/channel/UC73V8q795eSEMxDX4Pvdwmw/)
   * [bilibili](https://www.bilibili.com/video/av67454132)

<!--
## How can I get Nebula ##
Apart from installing **Nebula Graph** from source code, you can use the [official Nebula Graph image](https://hub.docker.com/r/vesoft/nebula-graph/tags). For more details on how to install Nebula Graph, see [Get Started](https://github.com/vesoft-inc/nebula/blob/master/docs/get-started.md).

## How can I contribute ##
As the team behind **Nebula Graph**, we fully commit to the community and all-in to the open source project. All the core features are and will be implemented in the open source repository.

We also encourage the community to involve the project. There are a few ways you can contribute:

* You can download and try **Nebula Graph**, and provide us feedbacks
* You can submit your feature requirements and bug reports
* You can help contribute the documentations. More details on how to contribute
click [Contribute to Nebula Graph Docs](https://github.com/vesoft-inc/nebula/blob/master/docs/contribute-to-documentation.md)
* You can fix bugs or implement features. More details on how to build the project and submit the Pull Requests click [Contribute to Nebula Graph](https://github.com/vesoft-inc/nebula/blob/master/docs/how-to-contribute.md)

## Licensing ###
**Nebula Graph** is under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license, so you can freely download, modify, deploy the source code to meet your needs. You can also freely deploy **Nebula Graph** as a back-end service to support your SAAS deployment.

In order to prevent cloud providers monetarizing from the project without contributing back, we added [Common Clause 1.0](https://commonsclause.com/) to the project. As mentioned above, we fully commit to the open source community. We would love to hear your thoughts on the licensing model and are willing to make it more suitable for the community.

## Contact
- Please use [GitHub issue tracker](https://github.com/vesoft-inc/nebula/issues) for reporting bugs or feature requests.
- Join [![](https://img.shields.io/badge/slack-nebula-519dd9.svg)](https://nebulagraph.slack.com/archives/DJQC9P0H5/p1557815158000200).
- Visit Nebula Graph [home page](http://nebula-graph.io/) for more features.
--!>
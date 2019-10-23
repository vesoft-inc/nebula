<!-- TODO 
# Welcome to the official Nebula Graph documentation.
---

**Nebula Graph** is a distributed, scalable, lightning-fast graph database. It is the only solution in the world capable to host graphs with dozens of billions of vertices (nodes) and trillions of edges, while still provides millisecond latency.

## Overview
---

* [**[Concepts](https://github.com/vesoft-inc/nebula/blob/master/README.md)**] An overall concept of Nebula Graph

* [**[Getting Started](https://github.com/vesoft-inc/nebula/blob/master/docs/get-started.md)**] Tutorial to help start with Nebula Graph
* [**[Query Language nGQL](https://github.com/vesoft-inc/nebula/blob/master/docs/nGQL-tutorial.md)**] Tutorial of Nebula Graph's query language nGQL
* [**[Data Model](https://github.com/vesoft-inc/nebula/blob/masterdo[**cs/manual-EN/overview/data-model.md)**]
* [**[Design and Arc](https://github.com/vesoft-inc/nebula/blobma[**ster/docs/manual-EN/overview/design-and-arc.md)**] Architecture information of Nebula Graph
* [**[Manual Example Graph](https://github.com/vesoft-inc/nebula/blobma[**ster/docs/manual-EN/overview/manual-example-graph.md)**]
* [**[Manual Introduction](https://github.com/vesoft-inc/nebula/blobma[**ster/docs/manual-EN/overview/manual-introduction.md)**]
* [**[Overview of Nebula Graph](https://github.com/vesoft-inc/nebulabl[**ob/master/docs/manual-EN/overview/overview-of-nebula-graph.md)**]


## User Manual
---
* Algorithm
  * [Find Path Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/algorithm/find-path-syntax.md)

* Data Types

  * [Numeric Types](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/data-types/numeric-types.md)
  * [String Types](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/data-types/string-types.md)
  * [Timestamp Types](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/data-types/.timestamp-types.md)


* Function and Operators

  * [Bitwise Operators](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/bitwise-operators.md)
  * [Comparison Functions and Operators](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/comparison-functions-and-operators.md)
  * [Functions and Operator Reference](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/functions-and-operator-reference.md)
  * [Group by Function](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/group-by-function.md)
  * [Limit Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/functions-and-operators/limit-syntax.md)
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

  * [Comment Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/comment-syntax.md)
  * [Expression](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/expression.md)
  * [Identifier Case Sensitivity](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/identifier-case-sensitivity.md)
  * [Keywords and Reserved Words](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/keywords-and-reserved-words.md)
  * [Property Reference](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/property-reference.md)
  * [Schema Object Names](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/schema-object-names.md)
  * [Statement Composition](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/statement-composition.md)
  * [User Defined Variable](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/language-structure/user-defined_variable.md)

* Server Administrations
  * Data Administration Statements
    * [Drop User Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/server-administrations/data-administration-statements/account-management-statements/drop-user-syntax.md)
  * Configuration Statements
    * [Variables Syntax](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/server-administrations/data-administration-statements/configuration-statements/variables-syntax.md)

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


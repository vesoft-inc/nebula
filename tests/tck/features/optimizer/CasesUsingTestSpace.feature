# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter Down Cases Using the test Space

  Background:
    Given a graph with space named "ngdata"

  Scenario: push filter down hash join bug fix
    When profiling query:
      """
      MATCH (v1:Label_6:Label_3)<-[e2:Rel_1]-(:Label_5)-[e3]->(v2)
      WHERE (id(v1) in [20, 28, 31, 6, 4, 18, 15, 25, 9, 19, 21])
      MATCH p0 = (v2)<-[e4]-()-[e5]->(v3:Label_6)
      WITH min(v3.Label_6.Label_6_4_Int) AS pa0,
      v3.Label_6.Label_6_1_Bool AS pa2
      WHERE pa2
      RETURN pa2
      """
    Then the result should be, in any order:
      | pa2  |
      | true |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                        |
      | 17 | project        | 19           |                                                                      |
      | 19 | aggregate      | 24           |                                                                      |
      | 24 | HashInnerJoin  | 21,29        |                                                                      |
      | 21 | project        | 6            |                                                                      |
      | 6  | AppendVertices | 26           |                                                                      |
      | 26 | Traverse       | 25           |                                                                      |
      | 25 | Traverse       | 2            |                                                                      |
      | 2  | Dedup          | 1            |                                                                      |
      | 1  | PassThrough    | 3            |                                                                      |
      | 3  | Start          |              |                                                                      |
      | 29 | project        | 27           |                                                                      |
      | 27 | AppendVertices | 11           | {"filter": "(Label_6.Label_6_1_Bool AND Label_6._tag IS NOT EMPTY)"} |
      | 11 | Traverse       | 10           |                                                                      |
      | 10 | Traverse       | 9            |                                                                      |
      | 9  | Argument       |              |                                                                      |

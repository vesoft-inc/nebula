# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Push Filter down LeftJoin rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down LeftJoin
    When profiling query:
      """
      LOOKUP ON player WHERE player.name=='Tim Duncan'
      | YIELD $-.VertexID AS vid
      |  GO FROM $-.vid OVER like BIDIRECT
      WHERE any(x in split($$.player.name, ' ') WHERE x contains 'Ti')
      YIELD $$.player.name, like._dst AS vid
      | GO FROM $-.vid OVER like BIDIRECT WHERE any(x in split($$.player.name, ' ') WHERE x contains 'Ti')
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name |
      | "Tim Duncan"   |
    And the execution plan should be:
      | id | name               | dependencies | operator info |
      | 22 | Project            | 21           |               |
      | 21 | Filter             | 20           |               |
      | 20 | InnerJoin          | 19           |               |
      | 19 | LeftJoin           | 18           |               |
      | 18 | Project            | 17           |               |
      | 17 | GetVertices        | 16           |               |
      | 16 | Project            | 28           |               |
      | 28 | GetNeighbors       | 12           |               |
      | 12 | Project            | 11           |               |
      | 11 | Filter             | 10           |               |
      | 10 | InnerJoin          | 9            |               |
      | 9  | LeftJoin           | 8            |               |
      | 8  | Project            | 7            |               |
      | 7  | GetVertices        | 6            |               |
      | 6  | Project            | 27           |               |
      | 27 | GetNeighbors       | 2            |               |
      | 2  | Project            | 3            |               |
      | 3  | Project            | 30           |               |
      | 30 | TagIndexPrefixScan | 0            |               |
      | 0  | Start              |              |               |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE $$.player.age >= 32 AND like.likeness > 85
      YIELD like._dst AS id, like.likeness AS likeness, $$.player.age AS age
      """
    Then the result should be, in any order:
      | id                  | likeness | age |
      | "LaMarcus Aldridge" | 90       | 33  |
      | "Manu Ginobili"     | 95       | 41  |
      | "Tim Duncan"        | 95       | 42  |
    And the execution plan should be:
      | id | name         | dependencies | operator info                               |
      | 7  | Project      | 6            |                                             |
      | 6  | Filter       | 5            | {"condition" : "($__COL_0>=32)"}            |
      | 5  | LeftJoin     | 8            |                                             |
      | 8  | Filter       | 4            | {"condition" : "($__Project_2.__COL_1>85)"} |
      | 4  | Project      | 3            |                                             |
      | 3  | GetVertices  | 2            |                                             |
      | 2  | Project      | 1            |                                             |
      | 1  | GetNeighbors | 0            |                                             |
      | 0  | Start        |              |                                             |

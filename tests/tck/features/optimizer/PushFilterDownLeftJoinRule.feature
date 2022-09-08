# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down LeftJoin rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down LeftJoin
    When profiling query:
      """
      LOOKUP ON player WHERE player.name=='Tim Duncan' YIELD id(vertex) as id
      | YIELD $-.id AS vid
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
      | 24 | Project            | 34           |               |
      | 34 | InnerJoin          | 33           |               |
      | 33 | Filter             | 21           |               |
      | 21 | LeftJoin           | 20           |               |
      | 20 | Project            | 19           |               |
      | 19 | GetVertices        | 18           |               |
      | 18 | Project            | 30           |               |
      | 30 | GetNeighbors       | 14           |               |
      | 14 | Project            | 32           |               |
      | 32 | InnerJoin          | 31           |               |
      | 31 | Filter             | 11           |               |
      | 11 | LeftJoin           | 10           |               |
      | 10 | Project            | 9            |               |
      | 9  | GetVertices        | 8            |               |
      | 8  | Project            | 29           |               |
      | 29 | GetNeighbors       | 26           |               |
      | 26 | Project            | 25           |               |
      | 25 | TagIndexPrefixScan | 0            |               |
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

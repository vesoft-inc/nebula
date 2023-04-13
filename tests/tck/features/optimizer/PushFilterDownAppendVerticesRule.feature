# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down AppendVertices rule
  Examples:
     | space_name  |
     | nba         |
     | nba_int_vid |

  Background:
    Given a graph with space named "<space_name>"

  Scenario: push filter down AppendVertices
    When profiling query:
      """
      MATCH (v:player) WHERE v.player.name == 'Tim Duncan' RETURN v.player.age AS age
      """
    Then the result should be, in any order:
      | age |
      | 42  |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                              |
      | 0  | Project        | 3            |                                                                            |
      | 3  | AppendVertices | 1            | {"filter": "((player.name==\"Tim Duncan\") AND player._tag IS NOT EMPTY)"} |
      | 1  | IndexScan      | 2            |                                                                            |
      | 2  | Start          |              |                                                                            |
    When profiling query:
      """
      MATCH (v:player)-[:like]->(p) WHERE p.player.name == 'Tim Duncan' RETURN p.player.age AS age
      """
    Then the result should be, in any order:
      | age |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
    And the execution plan should be:
      | id | name           | dependencies | operator info                               |
      | 0  | Project        | 3            |                                             |
      | 3  | AppendVertices | 4            | {"filter": "(player.name==\"Tim Duncan\")"} |
      | 4  | Traverse       | 1            |                                             |
      | 1  | IndexScan      | 2            |                                             |
      | 2  | Start          |              |                                             |
    When profiling query:
      """
      MATCH (v:player)-[:like*0..1]->(p) WHERE p.player.name == 'Tim Duncan' RETURN p.player.age AS age
      """
    Then the result should be, in any order:
      | age |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
    And the execution plan should be:
      | id | name           | dependencies | operator info                               |
      | 0  | Project        | 3            |                                             |
      | 3  | AppendVertices | 4            | {"filter": "(player.name==\"Tim Duncan\")"} |
      | 4  | Traverse       | 1            |                                             |
      | 1  | IndexScan      | 2            |                                             |
      | 2  | Start          |              |                                             |

  Scenario: push filter down AppendVertices with complex filter
    When profiling query:
      """
      MATCH (v:player)-[:like]-(p) WHERE v.player.age > 30 AND p.player.name == 'Tim Duncan' RETURN p.player.age AS age
      """
    Then the result should be, in any order:
      | age |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
      | 42  |
    And the execution plan should be:
      | id | name           | dependencies | operator info                               |
      | 0  | Project        | 4            |                                             |
      | 4  | Filter         | 3            | {"condition": "($-.v.player.age>30)"}       |
      | 3  | AppendVertices | 7            | {"filter": "(player.name==\"Tim Duncan\")"} |
      | 7  | Traverse       | 1            |                                             |
      | 1  | IndexScan      | 2            |                                             |
      | 2  | Start          |              |                                             |

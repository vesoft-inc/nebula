# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down Traverse rule
  Examples:
     | space_name  |
     | nba         |
     | nba_int_vid |

  Background:
    Given a graph with space named "<space_name>"

  Scenario: push filter down Traverse
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE e.likeness > 99 RETURN e.likeness, v2.player.age
      """
    Then the result should be, in any order:
      | e.likeness | v2.player.age |
      | 100        | 31            |
      | 99         | 34            |
      | 99         | 25            |
      | 99         | 34            |
      | 99         | 30            |
      | 99         | 31            |
      | 99         | 29            |
      | 99         | 38            |
      | 99         | 30            |
      | 100        | 43            |
      | 99         | 88            |
      | 99         | 32            |
      | 99         | 42            |
      | 99         | 33            |
      | 99         | 41            |
    And the execution plan should be:
      | id | name           | dependencies | operator info                         |
      | 7  | Project        | 6            |                                       |
      | 6  | Project        | 5            |                                       |
      | 5  | AppendVertices | 10           |                                       |
      | 10 | Traverse       | 2            | {"edge filter": "(like.likeness>99)"} |
      | 2  | Dedup          | 9            |                                       |
      | 9  | IndexScan      | 3            |                                       |
      | 3  | Start          |              |                                       |

  Scenario: push filter down Traverse with complex filter
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE v.player.age > 35 and e.likeness > 99 RETURN e.likeness, v2.player.age
      """
    Then the result should be, in any order:
      | e.likeness | v2.player.age |
      | 100        | 31            |
    And the execution plan should be:
      | id | name           | dependencies | operator info                          |
      | 10 | Project        | 9            |                                        |
      | 9  | Filter         | 4            | {"condition": }                        |
      | 4  | AppendVertices | 12           |                                        |
      | 12 | Traverse       | 8            | {"edge filter": "(like.likeness>99)" } |
      | 8  | IndexScan      | 2            |                                        |
      | 2  | Start          |              |                                        |

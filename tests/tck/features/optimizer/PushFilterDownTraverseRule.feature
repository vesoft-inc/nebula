# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down Traverse rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down Traverse
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE e.likeness > 99 RETURN e.likeness, v2.player.age
      """
    Then the result should be, in any order:
      | e.likeness | v2.player.age |
      | 100        | 31            |
      | 100        | 43            |
    # The filter `e.likeness>99` is first pushed down to the `eFilter_` of `Traverese` by rule `PushFilterDownTraverse`,
    # and then pushed down to the `filter_` of `Traverse` by rule `PushEFilterDown`.
    And the execution plan should be:
      | id | name           | dependencies | operator info                    |
      | 6  | Project        | 5            |                                  |
      | 5  | AppendVertices | 10           |                                  |
      | 10 | Traverse       | 2            | {"filter": "(like.likeness>99)"} |
      | 2  | Dedup          | 9            |                                  |
      | 9  | IndexScan      | 3            |                                  |
      | 3  | Start          |              |                                  |

  Scenario: push filter down Traverse with complex filter
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE v.player.age != 35 and e.likeness != 99
      RETURN e.likeness, v2.player.age as age
      ORDER BY age
      LIMIT 3
      """
    Then the result should be, in any order:
      | e.likeness | age |
      | 90         | 20  |
      | 80         | 22  |
      | 90         | 23  |
    # The filter `e.likeness!=99` is first pushed down to the `eFilter_` of `Traverese` by rule `PushFilterDownTraverse`,
    # and then pushed down to the `filter_` of `Traverse` by rule `PushEFilterDown`.
    And the execution plan should be:
      | id | name           | dependencies | operator info                          |
      | 11 | TopN           | 10           |                                        |
      | 10 | Project        | 9            |                                        |
      | 9  | Filter         | 4            | {"condition": "(-.v.player.age!=35)" } |
      | 4  | AppendVertices | 12           |                                        |
      | 12 | Traverse       | 8            | {"filter": "(like.likeness!=99)"}      |
      | 8  | IndexScan      | 2            |                                        |
      | 2  | Start          |              |                                        |

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Simple case

  Background:
    Given a graph with space named "nba"

  Scenario: go 1 steps yield distinct dst id
    When profiling query:
      """
      GO FROM "Tony Parker" OVER serve BIDIRECT YIELD DISTINCT id($$) as dst | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 2        |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 3  | Aggregate   | 2            |               |
      | 2  | Dedup       | 1            |               |
      | 1  | GetDstBySrc | 0            |               |
      | 0  | Start       |              |               |

  Scenario: go m steps
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT YIELD DISTINCT id($$) AS dst | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 22       |
    And the execution plan should be:
      | id | name        | dependencies | operator info     |
      | 7  | Aggregate   | 6            |                   |
      | 6  | Dedup       | 5            |                   |
      | 5  | GetDstBySrc | 4            |                   |
      | 4  | Loop        | 0            | {"loopBody": "3"} |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
    # The last step degenerates to `GetNeighbors` when yield or filter properties other than `_dst`
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT YIELD id($$) AS dst | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 65       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 7  | Aggregate    | 6            |                   |
      | 6  | Project      | 5            |                   |
      | 5  | GetNeighbors | 4            |                   |
      | 4  | Loop         | 0            | {"loopBody": "3"} |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT YIELD $$.player.age AS age | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 65       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 11 | Aggregate    | 10           |                   |
      | 10 | Project      | 9            |                   |
      | 9  | LeftJoin     | 8            |                   |
      | 8  | Project      | 7            |                   |
      | 7  | GetVertices  | 6            |                   |
      | 6  | Project      | 5            |                   |
      | 5  | GetNeighbors | 4            |                   |
      | 4  | Loop         | 0            | {"loopBody": "3"} |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER * WHERE $$.player.age > 36 YIELD $$.player.age AS age | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 10       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 12 | Aggregate    | 11           |                   |
      | 11 | Project      | 10           |                   |
      | 10 | Filter       | 9            |                   |
      | 9  | LeftJoin     | 8            |                   |
      | 8  | Project      | 7            |                   |
      | 7  | GetVertices  | 6            |                   |
      | 6  | Project      | 5            |                   |
      | 5  | GetNeighbors | 4            |                   |
      | 4  | Loop         | 0            | {"loopBody": "3"} |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |

  Scenario: go m to n steps yield distinct dst id
    When profiling query:
      """
      GO 1 to 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT YIELD DISTINCT id($$) AS dst | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 41       |
    And the execution plan should be:
      | id | name        | dependencies | operator info     |
      | 6  | Aggregate   | 5            |                   |
      | 5  | DataCollect | 4            |                   |
      | 4  | Loop        | 0            | {"loopBody": "3"} |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |

  Scenario: k-hop neighbors
    When profiling query:
      """
      $v1 = GO 1 to 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT YIELD DISTINCT id($$) as dst; $v2 = GO from $v1.dst OVER serve BIDIRECT YIELD DISTINCT id($$) as dst; (Yield $v2.dst as id minus yield $v1.dst as id) | yield count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 28       |
    And the execution plan should be:
      | id | name        | dependencies | operator info     |
      | 14 | Aggregate   | 12           |                   |
      | 12 | Minus       | 10,11        |                   |
      | 10 | Project     | 13           |                   |
      | 13 | PassThrough | 9            |                   |
      | 9  | Dedup       | 15           |                   |
      | 15 | GetDstBySrc | 5            |                   |
      | 5  | DataCollect | 4            |                   |
      | 4  | Loop        | 0            | {"loopBody": "3"} |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
      | 11 | Project     | 13           |                   |

  Scenario: other simple case
    When profiling query:
      """
      GO FROM "Tony Parker" OVER serve BIDIRECT YIELD DISTINCT id($$) as dst | GO FROM $-.dst OVER serve YIELD DISTINCT id($$) as dst | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 0        |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 7  | Aggregate   | 6            |               |
      | 6  | Dedup       | 5            |               |
      | 5  | GetDstBySrc | 4            |               |
      | 4  | Dedup       | 3            |               |
      | 3  | Project     | 2            |               |
      | 2  | Dedup       | 1            |               |
      | 1  | GetDstBySrc | 0            |               |
      | 0  | Start       |              |               |

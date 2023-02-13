# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Simple case

  Background:
    Given a graph with space named "nba"

  Scenario: go 1 step
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
    When profiling query:
      """
      GO FROM "Yao Ming" OVER like YIELD DISTINCT id($$) AS dst, $$.player.age AS age | ORDER BY $-.dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                | age |
      | "Shaquille O'Neal" | 47  |
      | "Tracy McGrady"    | 39  |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 8  | Sort         | 7            |               |
      | 7  | Project      | 6            |               |
      | 6  | HashLeftJoin | 2, 5         |               |
      | 2  | Dedup        | 1            |               |
      | 1  | GetDstBySrc  | 0            |               |
      | 0  | Start        |              |               |
      | 5  | Project      | 4            |               |
      | 4  | GetVertices  | 3            |               |
      | 3  | Argument     |              |               |
    When profiling query:
      """
      GO FROM "Yao Ming" OVER like WHERE $$.player.age > 40 YIELD DISTINCT id($$) AS dst, $$.player.age AS age | ORDER BY $-.dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                | age |
      | "Shaquille O'Neal" | 47  |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 9  | Sort         | 8            |               |
      | 8  | Project      | 7            |               |
      | 7  | Filter       | 6            |               |
      | 6  | HashLeftJoin | 2, 5         |               |
      | 2  | Dedup        | 1            |               |
      | 1  | GetDstBySrc  | 0            |               |
      | 0  | Start        |              |               |
      | 5  | Project      | 4            |               |
      | 4  | GetVertices  | 3            |               |
      | 3  | Argument     |              |               |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like YIELD DISTINCT id($$) AS a | ORDER BY $-.a
      """
    Then the result should be, in any order, with relax comparison:
      | a                   |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 3  | Sort        | 2            |               |
      | 2  | Dedup       | 1            |               |
      | 1  | GetDstBySrc | 0            |               |
      | 0  | Start       |              |               |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like YIELD DISTINCT 2, id($$) AS a | ORDER BY $-.a
      """
    Then the result should be, in any order, with relax comparison:
      | 2 | a                   |
      | 2 | "LaMarcus Aldridge" |
      | 2 | "Manu Ginobili"     |
      | 2 | "Tim Duncan"        |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 4  | Sort        | 3            |               |
      | 3  | Project     | 3            |               |
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
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT WHERE $$.team.name != "Lakers" YIELD DISTINCT id($$) | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 21       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 13 | Aggregate    | 12           |                   |
      | 12 | Project      | 11           |                   |
      | 11 | Filter       | 10           |                   |
      | 10 | HashLeftJoin | 6,9          |                   |
      | 6  | Dedup        | 5            |                   |
      | 5  | GetDstBySrc  | 4            |                   |
      | 4  | Loop         | 0            | {"loopBody": "3"} |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
      | 9  | Project      | 8            |                   |
      | 8  | GetVertices  | 7            |                   |
      | 7  | Argument     |              |                   |
    # The last step degenerates to `GetNeighbors` when the yield clause is not `YIELD DISTINCT id($$)`
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
      GO 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT YIELD DISTINCT $$.team.name, id($$) AS dst | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 22       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 12 | Aggregate    | 11           |                   |
      | 11 | Project      | 10           |                   |
      | 10 | HashLeftJoin | 6, 9         |                   |
      | 6  | Dedup        | 5            |                   |
      | 5  | GetDstBySrc  | 4            |                   |
      | 4  | Loop         | 0            | {"loopBody": "3"} |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
      | 9  | Project      | 8            |                   |
      | 8  | GetVertices  | 7            |                   |
      | 7  | Argument     |              |                   |
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT WHERE $^.player.age > 30 YIELD DISTINCT id($$) AS dst | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 22       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 9  | Aggregate    | 8            |                   |
      | 8  | Dedup        | 7            |                   |
      | 7  | Project      | 10           |                   |
      | 10 | GetNeighbors | 4            |                   |
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
      | 12 | Aggregate    | 11           |                   |
      | 11 | Project      | 10           |                   |
      | 10 | HashLeftJoin | 6,9          |                   |
      | 6  | Project      | 5            |                   |
      | 5  | GetNeighbors | 4            |                   |
      | 4  | Loop         | 0            | {"loopBody": "3"} |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
      | 9  | Project      | 8            |                   |
      | 8  | GetVertices  | 7            |                   |
      | 7  | Argument     |              |                   |
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER * WHERE $$.player.age > 36 YIELD $$.player.age AS age | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 10       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 13 | Aggregate    | 12           |                   |
      | 12 | Project      | 11           |                   |
      | 11 | Filter       | 10           |                   |
      | 10 | HashLeftJoin | 6,9          |                   |
      | 6  | Project      | 5            |                   |
      | 5  | GetNeighbors | 4            |                   |
      | 4  | Loop         | 0            | {"loopBody": "3"} |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
      | 9  | Project      | 8            |                   |
      | 8  | GetVertices  | 7            |                   |
      | 7  | Argument     |              |                   |
    When profiling query:
      """
      YIELD "Tony Parker" as a | GO 3 STEPS FROM $-.a OVER serve BIDIRECT YIELD DISTINCT $$.team.name, id($$) AS dst | YIELD COUNT(*)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(*) |
      | 22       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 15 | Aggregate    | 14           |                   |
      | 14 | Project      | 13           |                   |
      | 13 | HashLeftJoin | 9,12         |                   |
      | 9  | Dedup        | 8            |                   |
      | 8  | GetDstBySrc  | 7            |                   |
      | 7  | Loop         | 6            | {"loopBody": "6"} |
      | 6  | Dedup        | 5            |                   |
      | 5  | GetDstBySrc  | 4            |                   |
      | 4  | Start        |              |                   |
      | 3  | Dedup        | 16           |                   |
      | 16 | Project      | 0            |                   |
      | 0  | Start        |              |                   |
      | 12 | Project      | 11           |                   |
      | 11 | GetVertices  | 10           |                   |
      | 10 | Argument     |              |                   |
    # Because GetDstBySrc doesn't support limit push down, so degenerate to GetNeighbors when the query contains limit/simple clause
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER * YIELD DISTINCT id($$) LIMIT [100, 100, 100] | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 13       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 11 | Aggregate    | 10           |                   |
      | 10 | Dedup        | 9            |                   |
      | 9  | Project      | 12           |                   |
      | 12 | Limit        | 13           |                   |
      | 13 | GetNeighbors | 6            |                   |
      | 6  | Loop         | 0            | {"loopBody": "5"} |
      | 5  | Dedup        | 4            |                   |
      | 4  | Project      | 14           |                   |
      | 14 | Limit        | 15           |                   |
      | 15 | GetNeighbors | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |

  Scenario: go m to n steps
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
    When profiling query:
      """
      GO 1 to 3 STEP FROM "Tony Parker" OVER like WHERE $$.player.age > 40 YIELD DISTINCT id($$), $$.player.age as age, $$.player.name | ORDER BY $-.age
      """
    Then the result should be, in any order, with relax comparison:
      | id($$)          | age | $$.player.name  |
      | "Manu Ginobili" | 41  | "Manu Ginobili" |
      | "Tim Duncan"    | 42  | "Tim Duncan"    |
    And the execution plan should be:
      | id | name         | dependencies | operator info      |
      | 13 | Sort         | 12           |                    |
      | 12 | DataCollect  | 11           |                    |
      | 11 | Loop         | 0            | {"loopBody": "10"} |
      | 10 | Project      | 9            |                    |
      | 9  | Filter       | 8            |                    |
      | 8  | HashLeftJoin | 4,7          |                    |
      | 4  | Project      | 3            |                    |
      | 3  | Dedup        | 2            |                    |
      | 2  | GetDstBySrc  | 1            |                    |
      | 1  | Start        |              |                    |
      | 7  | Project      | 6            |                    |
      | 6  | GetVertices  | 5            |                    |
      | 5  | Argument     |              |                    |
      | 0  | Start        |              |                    |
    When profiling query:
      """
      GO 1 to 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT YIELD DISTINCT 3, id($$) AS dst | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 41       |
    And the execution plan should be:
      | id | name        | dependencies | operator info     |
      | 7  | Aggregate   | 6            |                   |
      | 6  | DataCollect | 5            |                   |
      | 5  | Loop        | 0            | {"loopBody": "4"} |
      | 4  | Project     | 3            |                   |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
    When profiling query:
      """
      GO 1 to 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT YIELD DISTINCT $$.player.age AS age, id($$) | YIELD COUNT($-.age)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT($-.age) |
      | 19            |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 12 | Aggregate    | 11           |                   |
      | 11 | DataCollect  | 10           |                   |
      | 10 | Loop         | 9            | {"loopBody": "9"} |
      | 9  | Project      | 8            |                   |
      | 8  | HashLeftJoin | 4,7          |                   |
      | 4  | Project      | 3            |                   |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 7  | Project      | 6            |                   |
      | 6  | GetVertices  | 5            |                   |
      | 5  | Argument     |              |                   |
      | 0  | Start        |              |                   |
    When profiling query:
      """
      GO 1 to 8 steps FROM "Tony Parker" OVER serve, like YIELD distinct like._dst AS a | YIELD COUNT($-.a)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT($-.a) |
      | 4           |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 9  | Aggregate    | 8            |                   |
      | 8  | DataCollect  | 7            |                   |
      | 7  | Loop         | 0            | {"loopBody": "6"} |
      | 6  | Dedup        | 5            |                   |
      | 5  | Project      | 4            |                   |
      | 4  | Dedup        | 3            |                   |
      | 3  | Project      | 2            |                   |
      | 2  | GetNeighbors | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
    When profiling query:
      """
      GO 1 to 8 steps FROM "Tony Parker" OVER serve, like YIELD DISTINCT serve._dst AS a | YIELD COUNT($-.a)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT($-.a) |
      | 3           |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 9  | Aggregate    | 8            |                   |
      | 8  | DataCollect  | 7            |                   |
      | 7  | Loop         | 0            | {"loopBody": "6"} |
      | 6  | Dedup        | 5            |                   |
      | 5  | Project      | 4            |                   |
      | 4  | Dedup        | 3            |                   |
      | 3  | Project      | 2            |                   |
      | 2  | GetNeighbors | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |

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
    When profiling query:
      """
      GO 1 STEP FROM "Tony Parker" OVER * YIELD distinct id($$) as id| GO 3 STEP FROM $-.id OVER * YIELD distinct id($$) | YIELD COUNT(*)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(*) |
      | 22       |
    And the execution plan should be:
      | id | name        | dependencies | operator info     |
      | 11 | Aggregate   | 10           |                   |
      | 10 | Dedup       | 9            |                   |
      | 9  | GetDstBySrc | 8            |                   |
      | 8  | Loop        | 4            | {"loopBody": "7"} |
      | 7  | Dedup       | 6            |                   |
      | 6  | GetDstBySrc | 5            |                   |
      | 5  | Start       |              |                   |
      | 4  | Dedup       | 3            |                   |
      | 3  | Project     | 2            |                   |
      | 2  | Dedup       | 1            |                   |
      | 1  | GetDstBySrc | 0            |                   |
      | 0  | Start       |              |                   |

  Scenario: could not be optimied cases
    When profiling query:
      """
      GO FROM "Yao Ming" OVER like WHERE $$.player.age > 40 AND id($$) != "Tony Parker" YIELD DISTINCT id($$) AS dst, id($$) AS dst2, $$.player.age + 100 AS age | ORDER BY $-.dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                | dst2               | age |
      | "Shaquille O'Neal" | "Shaquille O'Neal" | 147 |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 10 | Sort         | 9            |               |
      | 9  | Dedup        | 8            |               |
      | 8  | Project      | 7            |               |
      | 7  | Filter       | 6            |               |
      | 6  | HashLeftJoin | 2,5          |               |
      | 2  | Project      | 1            |               |
      | 1  | GetNeighbors | 0            |               |
      | 0  | Start        |              |               |
      | 5  | Project      | 4            |               |
      | 4  | GetVertices  | 3            |               |
      | 3  | Argument     |              |               |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like WHERE like._dst != "Tim Duncan" YIELD DISTINCT id($$), 2, like._dst AS a | ORDER BY $-.a
      """
    Then the result should be, in any order, with relax comparison:
      | id($$)              | 2 | a                   |
      | "LaMarcus Aldridge" | 2 | "LaMarcus Aldridge" |
      | "Manu Ginobili"     | 2 | "Manu Ginobili"     |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 5  | Sort         | 4            |               |
      | 4  | Dedup        | 3            |               |
      | 3  | Project      | 6            |               |
      | 6  | GetNeighbors | 0            |               |
      | 0  | Start        |              |               |
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT WHERE id($$) != "Not exists" YIELD DISTINCT id($$), $$.player.age | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 22       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 14 | Aggregate    | 13           |                   |
      | 13 | Dedup        | 12           |                   |
      | 12 | Project      | 11           |                   |
      | 11 | Filter       | 10           |                   |
      | 10 | HashLeftJoin | 6,9          |                   |
      | 6  | Project      | 5            |                   |
      | 5  | GetNeighbors | 4            |                   |
      | 4  | Loop         | 0            | {"loopBody": "3"} |
      | 3  | Dedup        | 2            |                   |
      | 2  | GetDstBySrc  | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
      | 9  | Project      | 8            |                   |
      | 8  | GetVertices  | 7            |                   |
      | 7  | Argument     |              |                   |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER serve, like WHERE serve._dst !="abc" YIELD DISTINCT id($$) AS a | ORDER BY $-.a
      """
    Then the result should be, in any order, with relax comparison:
      | a                   |
      | "Hornets"           |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Spurs"             |
      | "Tim Duncan"        |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 5  | Sort         | 4            |               |
      | 4  | Dedup        | 3            |               |
      | 3  | Project      | 6            |               |
      | 6  | GetNeighbors | 0            |               |
      | 0  | Start        |              |               |
    When profiling query:
      """
      GO 1 STEP FROM "Tony Parker" OVER like, serve REVERSELY WHERE id($$) != "Tim Duncan" YIELD DISTINCT id($$)  | YIELD  count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 4        |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 10 | Aggregate    | 9            |               |
      | 9  | Dedup        | 8            |               |
      | 8  | Project      | 7            |               |
      | 7  | Filter       | 6            |               |
      | 6  | HashLeftJoin | 2,5          |               |
      | 2  | Project      | 1            |               |
      | 1  | GetNeighbors | 0            |               |
      | 0  | Start        |              |               |
      | 5  | Project      | 4            |               |
      | 4  | GetVertices  | 3            |               |
      | 3  | Argument     |              |               |
    When profiling query:
      """
      GO 1 to 3 STEPS FROM "Tony Parker" OVER like WHERE like._dst != "Yao Ming" YIELD DISTINCT id($$) AS a | ORDER BY $-.a
      """
    Then the result should be, in any order, with relax comparison:
      | a                   |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
      | "Tony Parker"       |
    And the execution plan should be:
      | id | name         | dependencies | operator info     |
      | 10 | Sort         | 9            |                   |
      | 9  | DataCollect  | 8            |                   |
      | 8  | Loop         | 0            | {"loopBody": "7"} |
      | 7  | Dedup        | 6            |                   |
      | 6  | Project      | 5            |                   |
      | 5  | Filter       | 4            |                   |
      | 4  | Dedup        | 3            |                   |
      | 3  | Project      | 2            |                   |
      | 2  | GetNeighbors | 1            |                   |
      | 1  | Start        |              |                   |
      | 0  | Start        |              |                   |
    When profiling query:
      """
      GO 1 to 3 STEP FROM "Tony Parker" OVER like WHERE id($$) != "Tim Duncan" YIELD DISTINCT id($$), $$.player.age as age, $$.player.name | ORDER BY $-.age
      """
    Then the result should be, in any order, with relax comparison:
      | id($$)              | age | $$.player.name      |
      | "LaMarcus Aldridge" | 33  | "LaMarcus Aldridge" |
      | "Tony Parker"       | 36  | "Tony Parker"       |
      | "Manu Ginobili"     | 41  | "Manu Ginobili"     |
    And the execution plan should be:
      | id | name         | dependencies | operator info      |
      | 15 | Sort         | 14           |                    |
      | 14 | DataCollect  | 13           |                    |
      | 13 | Loop         | 0            | {"loopBody": "12"} |
      | 12 | Dedup        | 11           |                    |
      | 11 | Project      | 10           |                    |
      | 10 | Filter       | 9            |                    |
      | 9  | HashLeftJoin | 5,8          |                    |
      | 5  | Project      | 4            |                    |
      | 4  | Dedup        | 3            |                    |
      | 3  | Project      | 2            |                    |
      | 2  | GetNeighbors | 1            |                    |
      | 1  | Start        |              |                    |
      | 8  | Project      | 7            |                    |
      | 7  | GetVertices  | 6            |                    |
      | 6  | Argument     |              |                    |
      | 0  | Start        |              |                    |
    When profiling query:
      """
      GO FROM "Yao Ming" OVER like YIELD DISTINCT id($$) AS aa | GO 1 to 3 STEP FROM $-.aa OVER like WHERE id($$) != "Tim Duncan" YIELD DISTINCT id($$), $$.player.age as age, $$.player.name | ORDER BY $-.age
      """
    Then the result should be, in any order, with relax comparison:
      | id($$)              | age | $$.player.name      |
      | "JaVale McGee"      | 31  | "JaVale McGee"      |
      | "Rudy Gay"          | 32  | "Rudy Gay"          |
      | "LaMarcus Aldridge" | 33  | "LaMarcus Aldridge" |
      | "Tony Parker"       | 36  | "Tony Parker"       |
      | "Tracy McGrady"     | 39  | "Tracy McGrady"     |
      | "Kobe Bryant"       | 40  | "Kobe Bryant"       |
      | "Manu Ginobili"     | 41  | "Manu Ginobili"     |
      | "Grant Hill"        | 46  | "Grant Hill"        |
    And the execution plan should be:
      | id | name         | dependencies | operator info      |
      | 28 | Sort         | 27           |                    |
      | 27 | DataCollect  | 26           |                    |
      | 26 | Loop         | 10           | {"loopBody": "25"} |
      | 25 | Dedup        | 24           |                    |
      | 24 | Project      | 23           |                    |
      | 23 | Filter       | 22           |                    |
      | 22 | InnerJoin    | 21           |                    |
      | 21 | InnerJoin    | 20           |                    |
      | 20 | HashLeftJoin | 16,19        |                    |
      | 16 | Project      | 15           |                    |
      | 15 | Dedup        | 14           |                    |
      | 14 | Project      | 13           |                    |
      | 13 | InnerJoin    | 12           |                    |
      | 12 | Dedup        | 11           |                    |
      | 11 | Project      | 8            |                    |
      | 8  | Dedup        | 7            |                    |
      | 7  | Project      | 6            |                    |
      | 6  | GetNeighbors | 5            |                    |
      | 5  | Start        |              |                    |
      | 19 | Project      | 18           |                    |
      | 18 | GetVertices  | 17           |                    |
      | 17 | Argument     |              |                    |
      | 10 | Dedup        | 9            |                    |
      | 9  | Project      | 4            |                    |
      | 4  | Dedup        | 3            |                    |
      | 3  | Project      | 2            |                    |
      | 2  | Dedup        | 1            |                    |
      | 1  | GetDstBySrc  | 0            |                    |
      | 0  | Start        |              |                    |

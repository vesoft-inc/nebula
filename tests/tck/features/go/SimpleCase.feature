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
      | id | name        | dependencies | operator info |
      | 7  | Sort        | 6            |               |
      | 6  | Project     | 5            |               |
      | 5  | LeftJoin    | 4            |               |
      | 4  | Project     | 3            |               |
      | 3  | GetVertices | 2            |               |
      | 2  | Dedup       | 1            |               |
      | 1  | GetDstBySrc | 0            |               |
      | 0  | Start       |              |               |
    When profiling query:
      """
      GO FROM "Yao Ming" OVER like WHERE $$.player.age > 40 YIELD DISTINCT id($$) AS dst, $$.player.age AS age | ORDER BY $-.dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                | age |
      | "Shaquille O'Neal" | 47  |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 8  | Sort        | 7            |               |
      | 7  | Project     | 6            |               |
      | 6  | Filter      | 5            |               |
      | 5  | LeftJoin    | 4            |               |
      | 4  | Project     | 3            |               |
      | 3  | GetVertices | 2            |               |
      | 2  | Dedup       | 1            |               |
      | 1  | GetDstBySrc | 0            |               |
      | 0  | Start       |              |               |
    When profiling query:
      """
      GO FROM "Yao Ming" OVER like WHERE $$.player.age > 40 AND id($$) != "Tony Parker" YIELD DISTINCT id($$) AS dst, id($$) AS dst2, $$.player.age + 100 AS age | ORDER BY $-.dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                | dst2               | age |
      | "Shaquille O'Neal" | "Shaquille O'Neal" | 147 |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 8  | Sort        | 7            |               |
      | 7  | Project     | 11           |               |
      | 11 | Filter      | 10           |               |
      | 10 | LeftJoin    | 9            |               |
      | 9  | Filter      | 4            |               |
      | 4  | Project     | 3            |               |
      | 3  | GetVertices | 2            |               |
      | 2  | Dedup       | 1            |               |
      | 1  | GetDstBySrc | 0            |               |
      | 0  | Start       |              |               |
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
      | id | name        | dependencies | operator info |
      | 4  | Sort        | 3            |               |
      | 3  | Filter      | 3            |               |
      | 2  | Dedup       | 1            |               |
      | 1  | GetDstBySrc | 0            |               |
      | 0  | Start       |              |               |
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
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like WHERE like._dst != "Tim Duncan" YIELD DISTINCT id($$), 2, like._dst AS a | ORDER BY $-.a
      """
    Then the result should be, in any order, with relax comparison:
      | id($$)              | 2 | a                   |
      | "LaMarcus Aldridge" | 2 | "LaMarcus Aldridge" |
      | "Manu Ginobili"     | 2 | "Manu Ginobili"     |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 5  | Sort        | 4            |               |
      | 4  | Project     | 3            |               |
      | 3  | Filter      | 2            |               |
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
      | id | name        | dependencies | operator info     |
      | 12 | Aggregate   | 11           |                   |
      | 11 | Project     | 10           |                   |
      | 10 | Filter      | 9            |                   |
      | 9  | LeftJoin    | 8            |                   |
      | 8  | Project     | 7            |                   |
      | 7  | GetVertices | 6            |                   |
      | 6  | Dedup       | 5            |                   |
      | 5  | GetDstBySrc | 4            |                   |
      | 4  | Loop        | 0            | {"loopBody": "3"} |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
    When profiling query:
      """
      GO 3 STEPS FROM "Tony Parker" OVER serve BIDIRECT WHERE id($$) != "Not exists" YIELD DISTINCT id($$), $$.player.age | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 22       |
    And the execution plan should be:
      | id | name        | dependencies | operator info     |
      | 12 | Aggregate   | 11           |                   |
      | 11 | Project     | 14           |                   |
      | 14 | LeftJoin    | 13           |                   |
      | 13 | Filter      | 8            |                   |
      | 8  | Project     | 7            |                   |
      | 7  | GetVertices | 6            |                   |
      | 6  | Dedup       | 5            |                   |
      | 5  | GetDstBySrc | 4            |                   |
      | 4  | Loop        | 0            | {"loopBody": "3"} |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
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
      | id | name        | dependencies | operator info     |
      | 11 | Aggregate   | 10           |                   |
      | 10 | Project     | 9            |                   |
      | 9  | LeftJoin    | 8            |                   |
      | 8  | Project     | 7            |                   |
      | 7  | GetVertices | 6            |                   |
      | 6  | Dedup       | 5            |                   |
      | 5  | GetDstBySrc | 4            |                   |
      | 4  | Loop        | 0            | {"loopBody": "3"} |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
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
    When profiling query:
      """
      YIELD "Tony Parker" as a | GO 3 STEPS FROM $-.a OVER serve BIDIRECT YIELD DISTINCT $$.team.name, id($$) AS dst | YIELD COUNT(*)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(*) |
      | 22       |
    And the execution plan should be:
      | id | name        | dependencies | operator info     |
      | 14 | Aggregate   | 13           |                   |
      | 13 | Project     | 12           |                   |
      | 12 | LeftJoin    | 11           |                   |
      | 11 | Project     | 10           |                   |
      | 10 | GetVertices | 9            |                   |
      | 9  | Dedup       | 8            |                   |
      | 8  | GetDstBySrc | 7            |                   |
      | 7  | Loop        | 3            | {"loopBody": "6"} |
      | 6  | Dedup       | 5            |                   |
      | 5  | GetDstBySrc | 4            |                   |
      | 4  | Start       |              |                   |
      | 3  | Dedup       | 15           |                   |
      | 15 | Project     | 0            |                   |
      | 0  | Start       |              |                   |
    # Because GetDstBySrc doesn't support limit push down, so degenrate to GetNeighbors when the query contains limit/simple clause
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
      GO 1 to 3 STEPS FROM "Tony Parker" OVER like WHERE like._dst != "Yao Ming" YIELD DISTINCT id($$) AS a | ORDER BY $-.a
      """
    Then the result should be, in any order, with relax comparison:
      | a                   |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
      | "Tony Parker"       |
    And the execution plan should be:
      | id | name        | dependencies | operator info     |
      | 8  | Sort        | 7            |                   |
      | 7  | DataCollect | 6            |                   |
      | 6  | Loop        | 0            | {"loopBody": "5"} |
      | 5  | Filter      | 4            |                   |
      | 4  | Project     | 3            |                   |
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
      | id | name        | dependencies | operator info     |
      | 12 | Sort        | 11           |                   |
      | 11 | DataCollect | 10           |                   |
      | 10 | Loop        | 0            | {"loopBody": "9"} |
      | 9  | Project     | 8            |                   |
      | 8  | Filter      | 7            |                   |
      | 7  | LeftJoin    | 6            |                   |
      | 6  | Project     | 5            |                   |
      | 5  | GetVertices | 4            |                   |
      | 4  | Project     | 3            |                   |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
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
      | id | name        | dependencies | operator info     |
      | 12 | Sort        | 11           |                   |
      | 11 | DataCollect | 10           |                   |
      | 10 | Loop        | 0            | {"loopBody": "9"} |
      | 9  | Project     | 14           |                   |
      | 14 | LeftJoin    | 13           |                   |
      | 13 | Filter      | 6            |                   |
      | 6  | Project     | 5            |                   |
      | 5  | GetVertices | 4            |                   |
      | 4  | Project     | 3            |                   |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
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
      | id | name        | dependencies | operator info     |
      | 11 | Aggregate   | 10           |                   |
      | 10 | DataCollect | 9            |                   |
      | 9  | Loop        | 0            | {"loopBody": "8"} |
      | 8  | Project     | 7            |                   |
      | 7  | LeftJoin    | 6            |                   |
      | 6  | Project     | 5            |                   |
      | 5  | GetVertices | 4            |                   |
      | 4  | Project     | 3            |                   |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
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
      | 15 | Aggregate   | 13           |                   |
      | 13 | Minus       | 11,12        |                   |
      | 11 | Project     | 14           |                   |
      | 14 | PassThrough | 10           |                   |
      | 10 | Dedup       | 16           |                   |
      | 16 | GetDstBySrc | 6            |                   |
      | 6  | DataCollect | 5            |                   |
      | 5  | Loop        | 0            | {"loopBody": "4"} |
      | 4  | Project     | 3            |                   |
      | 3  | Dedup       | 2            |                   |
      | 2  | GetDstBySrc | 1            |                   |
      | 1  | Start       |              |                   |
      | 0  | Start       |              |                   |
      | 12 | Project     | 14           |                   |

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
      | id | name        | dependencies | operator info      |
      | 16 | Sort        | 15           |                    |
      | 15 | DataCollect | 14           |                    |
      | 14 | Loop        | 4            | {"loopBody": "13"} |
      | 13 | Project     | 18           |                    |
      | 18 | LeftJoin    | 17           |                    |
      | 17 | Filter      | 10           |                    |
      | 10 | Project     | 9            |                    |
      | 9  | GetVertices | 8            |                    |
      | 8  | Project     | 7            |                    |
      | 7  | Dedup       | 6            |                    |
      | 6  | GetDstBySrc | 5            |                    |
      | 5  | Start       |              |                    |
      | 4  | Dedup       | 3            |                    |
      | 3  | Project     | 2            |                    |
      | 2  | Dedup       | 1            |                    |
      | 1  | GetDstBySrc | 0            |                    |
      | 0  | Start       |              |                    |

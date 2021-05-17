# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: UnaryExpression

  Background:
    Given a graph with space named "nba"

  Scenario: UnaryExpression basic
    When executing query:
      """
      YIELD [1 IS NULL, 1.2 IS NULL, true IS NULL, [1, 2] IS NULL, null IS NULL] AS isNull
      """
    Then the result should be, in order:
      | isNull                             |
      | [false, false, false, false, true] |
    When executing query:
      """
      YIELD [1 IS NOT NULL, 1.2 IS NOT NULL, true IS NOT NULL, [1, 2] IS NOT NULL, null IS NOT NULL] AS isNotNull
      """
    Then the result should be, in order:
      | isNotNull                       |
      | [true, true, true, true, false] |
    When executing query:
      """
      RETURN [1 IS NULL, 1.2 IS NULL, true IS NULL, [1, 2] IS NULL, null IS NULL] AS isNull
      """
    Then the result should be, in order:
      | isNull                             |
      | [false, false, false, false, true] |
    When executing query:
      """
      RETURN [1 IS NOT NULL, 1.2 IS NOT NULL, true IS NOT NULL, [1, 2] IS NOT NULL, null IS NOT NULL] AS isNotNull
      """
    Then the result should be, in order:
      | isNotNull                       |
      | [true, true, true, true, false] |

  Scenario: UnaryExpression in match clause
    When executing query:
      """
      MATCH (v:player)
      WHERE v.name IS NULL AND v.age < 0
      RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                  |
      | ("Null1":player{age:-1,name:NULL}) |
      | ("Null2":player{age:-2,name:NULL}) |
      | ("Null3":player{age:-3,name:NULL}) |
      | ("Null4":player{age:-4,name:NULL}) |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.name IS NOT NULL AND v.age > 34
      RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                                                                           |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})                                               |
      | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                                                           |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})                                           |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})                                                       |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | ("David West" :player{age: 38, name: "David West"})                                                         |
      | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})                                                   |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                                                         |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             |

  Scenario: Unary reduce
    When profiling query:
      """
      MATCH (v:player) WHERE !!(v.age>=40)
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                           |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                                                           |
      | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})                                                   |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                                                         |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})                                               |
    And the execution plan should be:
      | id | name        | dependencies | operator info                                      |
      | 10 | Project     | 12           |                                                    |
      | 12 | Filter      | 7            |                                                    |
      | 7  | Project     | 6            |                                                    |
      | 6  | Project     | 5            |                                                    |
      | 5  | Filter      | 14           |                                                    |
      | 14 | GetVertices | 11           |                                                    |
      | 11 | IndexScan   | 0            | {"indexCtx": {"columnHints":{"scanType":"RANGE"}}} |
      | 0  | Start       |              |                                                    |

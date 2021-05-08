# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Match index selection

  Background:
    Given a graph with space named "nba"

  Scenario: and filter embeding
    When profiling query:
      """
      MATCH (v:player)
      WHERE v.name>"Tim Duncan" and  v.name<="Yao Ming"
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                         |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"}) |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})           |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})   |
    And the execution plan should be:
      | id | name        | dependencies | operator info                                                                                                          |
      | 10 | Project     | 13           |                                                                                                                        |
      | 13 | Filter      | 7            |                                                                                                                        |
      | 7  | Project     | 6            |                                                                                                                        |
      | 6  | Project     | 5            |                                                                                                                        |
      | 5  | Filter      | 15           |                                                                                                                        |
      | 15 | GetVertices | 11           |                                                                                                                        |
      | 11 | IndexScan   | 0            | {"indexCtx": {"columnHints":{"scanType":"RANGE","column":"name","beginValue":"\"Tim Duncan","endValue":"\"Yao Ming"}}} |
      | 0  | Start       |              |                                                                                                                        |

  Scenario: or filter embeding
    When profiling query:
      """
      MATCH (v:player)
      WHERE
        v.name<="Aron Baynes"
        or v.name>"Yao Ming"
        or v.name=="Kobe Bryant"
        or v.age>40
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                           |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})                                           |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                                                         |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                                                           |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})                                               |
    And the execution plan should be:
      | id | name        | dependencies | operator info                                    |
      | 10 | Project     | 13           |                                                  |
      | 13 | Filter      | 7            | {"condition":"!(hasSameEdgeInPath($-.__COL_0))"} |
      | 7  | Project     | 6            |                                                  |
      | 6  | Project     | 5            |                                                  |
      | 5  | Filter      | 15           |                                                  |
      | 15 | GetVertices | 11           |                                                  |
      | 11 | IndexScan   | 0            |                                                  |
      | 0  | Start       |              |                                                  |

  Scenario: degenerate to full tag scan
    When profiling query:
      """
      MATCH (v:player)-[:like]->(n)
      WHERE
        v.name<="Aron Baynes"
        or n.age>45
      RETURN v, n
      """
    Then the result should be, in any order:
      | v                                                                 | n                                                                                                           |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})         | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"}) | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                   | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})                                               |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    And the execution plan should be:
      | id | name         | dependencies | operator info                                                                                        |
      | 16 | Project      | 19           |                                                                                                      |
      | 19 | Filter       | 13           | { "condition": "((($v.name<=\"Aron Baynes\") OR ($n.age>45)) AND !(hasSameEdgeInPath($-.__COL_0)))"} |
      | 13 | Project      | 12           |                                                                                                      |
      | 12 | InnerJoin    | 11           |                                                                                                      |
      | 11 | Project      | 21           |                                                                                                      |
      | 21 | GetVertices  | 7            |                                                                                                      |
      | 7  | Filter       | 6            |                                                                                                      |
      | 6  | Project      | 5            |                                                                                                      |
      | 5  | Filter       | 23           |                                                                                                      |
      | 23 | GetNeighbors | 17           |                                                                                                      |
      | 17 | IndexScan    | 0            |                                                                                                      |
      | 0  | Start        |              |                                                                                                      |
    # This is actually the optimization for another optRule,
    # but it is necessary to ensure that the current optimization does not destroy this scenario
    # and it can be considered in the subsequent refactoring
    When profiling query:
      """
      MATCH (v:player)-[:like]->(n)
      WHERE
        v.name<="Aron Baynes"
        or v.age>45
        or true
        or v.age+1
        or v.name
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 81    |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 16 | Aggregate    | 18           |               |
      | 18 | Filter       | 13           |               |
      | 13 | Project      | 12           |               |
      | 12 | InnerJoin    | 11           |               |
      | 11 | Project      | 20           |               |
      | 20 | GetVertices  | 7            |               |
      | 7  | Filter       | 6            |               |
      | 6  | Project      | 5            |               |
      | 5  | Filter       | 22           |               |
      | 22 | GetNeighbors | 17           |               |
      | 17 | IndexScan    | 0            |               |
      | 0  | Start        |              |               |

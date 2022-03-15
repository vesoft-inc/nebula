# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Match index selection

  Background:
    Given a graph with space named "nba"

  Scenario: and filter embedding
    When profiling query:
      """
      MATCH (v:player)
      WHERE v.player.name>"Tim Duncan" and  v.player.name<="Yao Ming"
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                         |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"}) |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})           |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})   |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                         |
      | 9  | Project        | 8            |                                                                                                                                                                       |
      | 8  | Filter         | 2            |                                                                                                                                                                       |
      | 2  | AppendVertices | 6            |                                                                                                                                                                       |
      | 6  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"RANGE","column":"name","beginValue":"\"Tim Duncan\"","endValue":"\"Yao Ming\"","includeBegin":"false","includeEnd":"true"}}} |
      | 0  | Start          |              |                                                                                                                                                                       |
    When profiling query:
      """
      MATCH (v:player)
      WHERE v.player.age>30 and v.player.age<=40
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                 |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"}) |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})             |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})         |
      | ("Chris Paul" :player{age: 33, name: "Chris Paul"})               |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               |
      | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     |
      | ("David West" :player{age: 38, name: "David West"})               |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                   |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | ("Stephen Curry" :player{age: 31, name: "Stephen Curry"})         |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})               |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             |
      | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"})               |
      | ("Rajon Rondo" :player{age: 33, name: "Rajon Rondo"})             |
      | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})     |
      | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})             |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                   |
      | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})         |
      | ("JaVale McGee" :player{age: 31, name: "JaVale McGee"})           |
      | ("Dwight Howard" :player{age: 33, name: "Dwight Howard"})         |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                  |
      | 9  | Project        | 8            |                                                                                                                                                |
      | 8  | Filter         | 2            |                                                                                                                                                |
      | 2  | AppendVertices | 6            |                                                                                                                                                |
      | 6  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"RANGE","column":"age","beginValue":"30","endValue":"40","includeBegin":"false","includeEnd":"true"}}} |
      | 0  | Start          |              |                                                                                                                                                |

  Scenario: or filter embedding
    When profiling query:
      """
      MATCH (v:player)
      WHERE
      v.player.name<="Aron Baynes"
      or v.player.name>"Yao Ming"
      or v.player.name=="Kobe Bryant"
      or v.player.age>40
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
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 6  | Project        | 2            |               |
      | 9  | Filter         | 3            |               |
      | 3  | AppendVertices | 7            |               |
      | 7  | IndexScan      | 2            |               |
      | 2  | Start          |              |               |

  Scenario: degenerate to full tag scan
    When profiling query:
      """
      MATCH (v:player)-[:like]->(n)
      WHERE
      v.player.name<="Aron Baynes"
      or n.player.age>45
      RETURN v, n
      """
    Then the result should be, in any order:
      | v                                                                 | n                                                                                                           |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})         | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"}) | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                   | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 9  | Project        | 8            |               |
      | 8  | Filter         | 3            |               |
      | 3  | AppendVertices | 2            |               |
      | 2  | Traverse       | 1            |               |
      | 1  | IndexScan      | 0            |               |
      | 0  | Start          |              |               |
    # This is actually the optimization for another optRule,
    # but it is necessary to ensure that the current optimization does not destroy this scenario
    # and it can be considered in the subsequent refactoring
    When profiling query:
      """
      MATCH (v:player)-[:like]->(n)
      WHERE
      v.player.name<="Aron Baynes"
      or v.player.age>45
      or true
      or v.player.age+1
      or v.player.name
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 81    |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 6  | Aggregate      | 8            |               |
      | 8  | Project        | 7            |               |
      | 7  | Filter         | 3            |               |
      | 3  | AppendVertices | 2            |               |
      | 2  | Traverse       | 1            |               |
      | 1  | IndexScan      | 0            |               |
      | 0  | Start          |              |               |

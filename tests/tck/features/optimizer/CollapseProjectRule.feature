# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Collapse Project Rule

  Background:
    Given a graph with space named "nba"

  @czp
  Scenario: Collapse Project
    When profiling query:
      """
      MATCH (v:player)
      WHERE v.player.age > 38
      WITH v, v.player.age AS age, v.player.age/10 AS iage, v.player.age%10 AS mage,v.player.name AS name
      RETURN iage
      """
    Then the result should be, in any order:
      | iage |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 3    |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 12 | Project        | 10           |               |
      | 10 | Filter         | 2            |               |
      | 2  | AppendVertices | 7            |               |
      | 7  | IndexScan      | 0            |               |
      | 0  | Start          |              |               |
    When profiling query:
      """
      LOOKUP ON player WHERE player.name=='Tim Duncan' YIELD id(vertex) as id
      | YIELD $-.id AS vid
      """
    Then the result should be, in any order:
      | vid          |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name               | dependencies | operator info |
      | 6  | Project            | 5            |               |
      | 5  | TagIndexPrefixScan | 0            |               |
      | 0  | Start              |              |               |
    When profiling query:
      """
      MATCH (:player {name:"Tim Duncan"})-[e:like]->(dst)
      WITH dst AS b
      RETURN b.player.age AS age
      """
    Then the result should be, in any order:
      | age |
      | 36  |
      | 41  |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                       |
      | 11 | Project        | 4            |                                                                                                                                                                     |
      | 4  | AppendVertices | 3            |                                                                                                                                                                     |
      | 3  | Traverse       | 8            |                                                                                                                                                                     |
      | 8  | IndexScan      | 2            | {"indexCtx": {"columnHints":{"scanType":"PREFIX","column":"name","beginValue":"\"Tim Duncan\"","endValue":"__EMPTY__","includeBegin":"true","includeEnd":"false"}}} |
      | 2  | Start          |              |                                                                                                                                                                     |
    When profiling query:
      """
      MATCH (v1:player)-[:like*1..2]-(v2:player)-[:serve]->(v3:team)
      WHERE id(v1)=="Tim Duncan" and v3.team.name < "z"
      RETURN DISTINCT v2.player.age AS vage ,v2.player.name AS vname
      """
    Then the result should be, in any order, with relax comparison:
      | vage | vname               |
      | 31   | "JaVale McGee"      |
      | 47   | "Shaquille O'Neal"  |
      | 31   | "Danny Green"       |
      | 32   | "Rudy Gay"          |
      | 25   | "Kyle Anderson"     |
      | 34   | "LeBron James"      |
      | 33   | "Chris Paul"        |
      | 28   | "Damian Lillard"    |
      | 33   | "LaMarcus Aldridge" |
      | 32   | "Marco Belinelli"   |
      | 29   | "James Harden"      |
      | 30   | "Kevin Durant"      |
      | 30   | "Russell Westbrook" |
      | 36   | "Boris Diaw"        |
      | 36   | "Tony Parker"       |
      | 32   | "Aron Baynes"       |
      | 38   | "Yao Ming"          |
      | 34   | "Tiago Splitter"    |
      | 41   | "Manu Ginobili"     |
      | 42   | "Tim Duncan"        |
      | 29   | "Dejounte Murray"   |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 10 | Dedup          | 14           |               |
      | 14 | Project        | 12           |               |
      | 12 | Filter         | 6            |               |
      | 6  | AppendVertices | 5            |               |
      | 5  | Filter         | 5            |               |
      | 5  | Traverse       | 4            |               |
      | 4  | Traverse       | 2            |               |
      | 2  | Dedup          | 1            |               |
      | 1  | PassThrough    | 3            |               |
      | 3  | Start          |              |               |

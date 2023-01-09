# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test extract filter

  Background:
    Given a graph with space named "nba"

  Scenario: Extract filter bug
    When executing query:
      """
      match p=allShortestPaths((v1)-[:like*1..3]-(v2))
      where id(v1)=="Tim Duncan" and id(v2)=="Tony Parker"
      with nodes(p) as pathNodes
      WITH
        [n IN pathNodes | id(n)] AS personIdsInPath,
        [idx IN range(1, size(pathNodes)-1) | [prev IN [pathNodes[idx-1]] | [curr IN [pathNodes[idx]] | [prev, curr]]]] AS vertList
      UNWIND vertList AS c
      WITH c[0][0][0] AS prev , c[0][0][1] AS curr
      OPTIONAL MATCH (curr)<-[e:like]-(:player)-[:serve]->(:team)-[:teammate]->(prev)
      RETURN count(e) AS cnt1, prev, curr
      """
    Then the result should be, in any order:
      | cnt1 | prev                                                                                                        | curr                                                  |
      | 0    | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
    When executing query:
      """
      match p=(a:player)-[e:like*1..3]->(b)
      where b.player.age>42
      with relationships(p)[1] AS e1
      match (b)-[:serve]->(c)
      where c.team.name>"S" and (b)-[e1]->()
      return count(c)
      """
    Then the result should be, in any order:
      | count(c) |
      | 49       |
    When executing query:
      """
      match p=(a:player)-[e:like*1..3]->(b)
      where b.player.age>42
      with relationships(p)[1..2][0] AS e1
      match (b)-[:serve]->(c)
      where c.team.name>"S" and (b)-[e1]->()
      return count(c)
      """
    Then the result should be, in any order:
      | count(c) |
      | 49       |

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test match used in pipe

  Background:
    Given a graph with space named "nba"

  Scenario: Multiple Match joined on edge
    When executing query:
      """
      MATCH (v:player)-[e:like*1..2]->(u) WHERE v.player.name=="Tim Duncan" MATCH (vv:player)-[e:like]->() WHERE vv.player.name=="Tony Parker"return v, u
      """
    Then a SemanticError should be raised at runtime: e binding to different type: Edge vs EdgeList
    When executing query:
      """
      MATCH (v:player)-[e:like*2]->(u) WHERE v.player.name=="Tim Duncan" MATCH (vv:player)-[e:like]->() WHERE vv.player.name=="Tony Parker"return v, u
      """
    Then a SemanticError should be raised at runtime: e binding to different type: Edge vs EdgeList
    When executing query:
      """
      MATCH (v:player)-[e:like]->() WHERE v.player.name=="Tim Duncan" MATCH (u:player)-[e:like]->() WHERE u.player.name=="Tony Parker"return v
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      MATCH (v:player)-[e:like]->() WHERE v.player.name=="Tim Duncan" MATCH ()-[e:like]->(u:player) WHERE u.player.name=="Tony Parker"return v, u
      """
    Then the result should be, in any order:
      | v                                                                                                           | u                                                     |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |

  Scenario: Multiple Match joined on edge list
    When executing query:
      """
      MATCH p1=(:player{name:"Tim Duncan"})-[e:like*1..2]->(:player{name:"Tim Duncan"})
      MATCH p2=(:player{name:"Tim Duncan"})-[e:like*1..2]-(:player{name:"Tim Duncan"})
      RETURN e[0].likeness AS e1, e[1].likeness AS e2
      """
    Then the result should be, in any order:
      | e1 | e2 |
      | 95 | 90 |
      | 95 | 95 |
    When executing query:
      """
      MATCH p1=(:player{name:"Tim Duncan"})-[e:like*1..2]->(:player{name:"Tim Duncan"})
      WITH relationships(p1) AS e
      MATCH p2=(:player{name:"Tim Duncan"})-[e:like*1..2]-(:player{name:"Tim Duncan"})
      WITH relationships(p2) AS e
      RETURN e[0].likeness AS e1, e[1].likeness AS e2
      """
    Then the result should be, in any order:
      | e1 | e2 |
      | 95 | 90 |
      | 95 | 95 |
    # issue #1538
    When executing query:
      """
      MATCH p1=(v1:player)-[e:like*1..]->(v) where id(v1)=='Tim Duncan'
      MATCH p2=(v2:player)-[e:like*1..]->(v) where id(v2)=='Tim Duncan'
      WITH id(v1) AS tagc1, id(v2) AS tagc2,
      sum(reduce(total=1,ratio in relationships(p1)|total * ratio.likeness)) AS ratio1,
      sum(reduce(total=1,ratio in relationships(p2)|total * ratio.likeness)) AS ratio2
      RETURN tagc1,tagc2, ratio1, ratio2;
      """
    Then the result should be, in any order:
      | tagc1        | tagc2        | ratio1        | ratio2        |
      | "Tim Duncan" | "Tim Duncan" | 2111707594715 | 2111707594715 |

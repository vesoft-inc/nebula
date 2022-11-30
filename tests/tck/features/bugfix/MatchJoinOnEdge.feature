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

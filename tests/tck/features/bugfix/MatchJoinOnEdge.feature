# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test match used in pipe

  Background:
    Given a graph with space named "nba"

  Scenario: Multiple Match joined on edge
    When executing query:
      """
      MATCH (v:player)-[e:follow*1..2]->(u) WHERE v.player.name=="Tim Duncan" MATCH (vv:player)-[e]->() WHERE vv.player.name=="Tony Parker"return v, u
      """
    Then a SemanticError should be raised at runtime: e binding to different type: 1 vs 3
    When executing query:
      """
      MATCH (v:player)-[e]->() WHERE v.player.name=="Tim Duncan" MATCH (u:player)-[e]->() WHERE u.player.name=="Tony Parker"return v
      """
    Then the result should be, in any order:
      | v |
      |   |
    When executing query:
      """
      MATCH (v:player)-[e]->() WHERE v.player.name=="Tim Duncan" MATCH ()-[e]->(u:player) WHERE u.player.name=="Tony Parker"return v, u
      """
    Then the result should be, in any order:
      | v                                                  | u                                                   |
      | ("player100" :player{age: 42, name: "Tim Duncan"}) | ("player101" :player{age: 36, name: "Tony Parker"}) |
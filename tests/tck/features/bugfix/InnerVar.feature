# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Inner Var Conflict

  Background:
    Given a graph with space named "nba"

  # The edge range will use same hard code innner variable name for list comprehension
  # It's not stable to reproduce, so run multiple times
  Scenario: Conflict hard code edge inner variable
    When executing query 100 times:
      """
      MATCH (v)-[:like*1..2]->(v2) WHERE id(v) == 'Tim Duncan'
      MATCH (v)-[:serve*1..2]->(t)
      RETURN v.player.name, v2.player.name, t.team.name
      """
    Then the result should be, in any order:
      | v.player.name | v2.player.name      | t.team.name |
      | "Tim Duncan"  | "Tony Parker"       | "Spurs"     |
      | "Tim Duncan"  | "Manu Ginobili"     | "Spurs"     |
      | "Tim Duncan"  | "LaMarcus Aldridge" | "Spurs"     |
      | "Tim Duncan"  | "Tim Duncan"        | "Spurs"     |
      | "Tim Duncan"  | "Tim Duncan"        | "Spurs"     |
      | "Tim Duncan"  | "Manu Ginobili"     | "Spurs"     |

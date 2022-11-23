# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test extract filter

  Background:
    Given a graph with space named "nba"

  Scenario: Extract filter bug
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like]->(t) WHERE ( (1 == 1 AND (NOT is_edge(t))) OR (v.player.name == 'Tim Duncan')) RETURN v.player.name
      """
    Then the result should be, in any order:
      | v.player.name |
      | 'Tim Duncan'  |
      | 'Tim Duncan'  |

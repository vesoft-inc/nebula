# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test find start error for property index seek

  Background:
    Given a graph with space named "nba"

  # #4763
  Scenario: Find start of match pattern
    When executing query:
      """
      MATCH (v:player) WHERE (1 + 1) == 3 RETURN v.player.name;
      """
    Then the result should be, in any order:
      | v.player.name |

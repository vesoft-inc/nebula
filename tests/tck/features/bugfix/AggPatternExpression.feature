# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
# #4175
Feature: Test crash when aggregate with pattern expression

  Background:
    Given a graph with space named "nba"

  Scenario: Crash when aggregate with pattern expression
    # TODO aggregate should bypass all input, like `(v)--(:team)` here
    When executing query:
      """
      MATCH (v:player) WHERE id(v) == 'Tim Duncan' return v.player.name AS name, size((v)--(:team)) + count(v.player.name) * 2 AS count
      """
    Then the result should be, in any order, with relax comparison:
      | name         | count |
      | 'Tim Duncan' | 3     |

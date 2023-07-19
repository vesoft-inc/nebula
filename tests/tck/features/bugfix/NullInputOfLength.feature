# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Null input of length function

  Background:
    Given a graph with space named "nba"

  Scenario: Null input of length function
    When executing query:
      """
      match p = (v)-[:like]->() where id(v) == 'Tim Duncan'
      return length(v.player.name) as l, length(p) as lp
      """
    Then the result should be, in any order:
      | l  | lp |
      | 10 | 1  |
      | 10 | 1  |

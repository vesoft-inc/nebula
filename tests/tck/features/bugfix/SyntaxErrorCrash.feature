# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
# #2780 ent
Feature: Test crash double delete expr

  Background:
    Given a graph with space named "nba"

  Scenario: crash double delete expr
    When executing query:
      """
      match (v.player) return v;
      """
    Then a SyntaxError should be raised at runtime:  Invalid node pattern near `(v.player)'
    When executing query:
      """
      match (v.player) return v;
      """
    Then a SyntaxError should be raised at runtime:  Invalid node pattern near `(v.player)'
    When executing query:
      """
      match (v.player) return v;
      """
    Then a SyntaxError should be raised at runtime:  Invalid node pattern near `(v.player)'
    When executing query:
      """
      return 1;
      """
    Then the result should be, in any order:
      | 1 |
      | 1 |

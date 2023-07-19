# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Variable usage

  Background:
    Given a graph with space named "nba"

  Scenario: disable yield $var
    When executing query:
      """
      $var = yield 1;$var2 = yield 3;yield $var1 + $var2
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `$var1 + $var2'
    When executing query:
      """
      $var=go from "Tim Duncan" over like yield like._dst as dst;yield $var
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `$var'
    Then drop the used space
    When executing query:
      """
      $var=go from "Tim Duncan" over like yield like._dst as dst;yield $var[0][0]
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `$var[0][0]'

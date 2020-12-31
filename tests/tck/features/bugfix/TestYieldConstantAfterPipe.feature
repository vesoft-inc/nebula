# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Test yield constant after pipe

  Background:
    Given a graph with space named "nba"

  Scenario: yield constant after pipe
    When executing query:
      """
      GO FROM "Tim Duncan" OVER * | YIELD 1 AS a;
      """
    Then the result should be, in any order:
      | a |
      | 1 |
      | 1 |
      | 1 |
      | 1 |
      | 1 |
      | 1 |
      | 1 |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER * | YIELD 1 AS a WHERE true;
      """
    Then the result should be, in any order:
      | a |
      | 1 |
      | 1 |
      | 1 |
      | 1 |
      | 1 |
      | 1 |
      | 1 |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER * | YIELD 1 AS a WHERE false;
      """
    Then the result should be, in any order:
      | a |

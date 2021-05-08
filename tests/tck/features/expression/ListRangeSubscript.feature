# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: ListRangeSubscript

  Scenario: [1] List slice with parameterised range
    Given any graph
    When executing query:
      """
      $var = YIELD 1 AS f, 3 AS t;
      YIELD [1, 2, 3][$var.f..$var.t] AS r;
      """
    Then the result should be, in any order:
      | r      |
      | [2, 3] |
    And no side effects

  Scenario: [2] List slice with parameterised invalid range
    Given any graph
    When executing query:
      """
      $var = YIELD 3 AS f, 1 AS t;
      YIELD [1, 2, 3][$var.f..$var.t] AS r;
      """
    Then the result should be, in any order:
      | r  |
      | [] |
    And no side effects

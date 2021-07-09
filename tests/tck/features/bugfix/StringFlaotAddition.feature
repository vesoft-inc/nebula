# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Test return float plus string

  Background:
    Given a graph with space named "nba"

  # Fix https://github.com/vesoft-inc/nebula-graph/issues/1214
  Scenario: addition[1]
    When executing query:
      """
      RETURN 30.142857142857142 + "Yao Ming"
      """
    Then the result should be, in any order, with relax comparison:
      | (30.142857142857142+"Yao Ming") |
      | "30.142857142857142Yao Ming"    |
    When executing query:
      """
      RETURN -30.142857142857142 + "Yao Ming"
      """
    Then the result should be, in any order, with relax comparison:
      | (-(30.142857142857142)+"Yao Ming") |
      | "-30.142857142857142Yao Ming"      |
    When executing query:
      """
      RETURN "Yao Ming" + 30.142857142857142
      """
    Then the result should be, in any order, with relax comparison:
      | ("Yao Ming"+30.142857142857142) |
      | "Yao Ming30.142857142857142"    |
    When executing query:
      """
      RETURN "Yao Ming" + -30.142857142857142
      """
    Then the result should be, in any order, with relax comparison:
      | ("Yao Ming"+-(30.142857142857142)) |
      | "Yao Ming-30.142857142857142"      |

  Scenario: addition[2]
    When executing query:
      """
      RETURN 30.14 + "Yao Ming"
      """
    Then the result should be, in any order, with relax comparison:
      | (30.14+"Yao Ming") |
      | "30.14Yao Ming"    |
    When executing query:
      """
      RETURN -30.14 + "Yao Ming"
      """
    Then the result should be, in any order, with relax comparison:
      | (-(30.14)+"Yao Ming") |
      | "-30.14Yao Ming"      |
    When executing query:
      """
      RETURN "Yao Ming" + 30.14
      """
    Then the result should be, in any order, with relax comparison:
      | ("Yao Ming"+30.14) |
      | "Yao Ming30.14"    |
    When executing query:
      """
      RETURN "Yao Ming" + -30.14
      """
    Then the result should be, in any order, with relax comparison:
      | ("Yao Ming"+-(30.14)) |
      | "Yao Ming-30.14"      |

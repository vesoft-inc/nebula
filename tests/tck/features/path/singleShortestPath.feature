# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Single Shortest Path

  Background:
    Given a graph with space named "nba"

  Scenario: Single Shortest Path zero step
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan" , "Yao Ming" TO "Tony Parker" OVER like UPTO 0 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan", "Yao Ming" TO "Tony Parker", "Spurs" OVER * UPTO 0 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |

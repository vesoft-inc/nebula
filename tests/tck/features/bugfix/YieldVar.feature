# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Test yield var

  Background:
    Given a graph with space named "nba"

  # #2646
  Scenario: yield constant after pipe
    When executing query:
      """
      $var = GO FROM "Tim Duncan" OVER *; YIELD $var[0][0] as var00;
      """
    Then the result should be, in any order:
      | var00           |
      | "Manu Ginobili" |
      | "Manu Ginobili" |
      | "Manu Ginobili" |
      | "Manu Ginobili" |
      | "Manu Ginobili" |
      | "Manu Ginobili" |
      | "Manu Ginobili" |

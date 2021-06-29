# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Test yield constant after pipe

  # Issue #1171
  Scenario: List slicing with null range
    When executing query:
      """
      return [1, 2, 3][null..1] as a
      """
    Then the result should be, in any order:
      | a    |
      | NULL |

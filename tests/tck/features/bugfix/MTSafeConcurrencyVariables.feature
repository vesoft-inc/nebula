# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test MT-safe variables

  Scenario: Binary plan of minus
    # It's not stable to reproduce the bug, so we run it 100 times
    When executing query 100 times:
      """
      YIELD 1 AS number MINUS YIELD 2 AS number
      """
    Then the result should be, in any order:
      | number |
      | 1      |

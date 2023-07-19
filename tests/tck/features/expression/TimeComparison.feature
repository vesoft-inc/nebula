# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Time computation

  Background:
    Given a graph with space named "nba"

  Scenario Outline: duration compare
    When executing query:
      """
      WITH duration(<lhs>) as x, duration(<rhs>) as d
      RETURN x > d AS gt, x < d AS lt, x == d AS eq, x != d AS ne,
      x >= d AS ge, x <= d AS le
      """
    Then the result should be, in any order:
      | gt            | lt         | eq      | ne         | ge             | le          |
      | <greaterThan> | <lessThan> | <equal> | <notEqual> | <greaterEqual> | <lessEqual> |

    Examples:
      | lhs                   | rhs                   | lessThan | greaterThan | equal | notEqual | greaterEqual | lessEqual |
      | {days: 30}            | {months: 1}           | BAD_TYPE | BAD_TYPE    | false | true     | BAD_TYPE     | BAD_TYPE  |
      | {days: 30, months: 1} | {days: 30, months: 1} | BAD_TYPE | BAD_TYPE    | true  | false    | BAD_TYPE     | BAD_TYPE  |

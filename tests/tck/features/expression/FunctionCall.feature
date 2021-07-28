# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Function Call Expression

  Background:
    Given a graph with space named "nba"

  Scenario: sign
    When executing query:
      """
      YIELD sign(38) AS a, sign(-2) AS b, sign(0.421) AS c,
            sign(-1.0) AS d, sign(0) AS e, sign(abs(-3)) AS f
      """
    Then the result should be, in any order:
      | a | b  | c | d  | e | f |
      | 1 | -1 | 1 | -1 | 0 | 1 |

  Scenario: date related
    When executing query:
      """
      YIELD timestamp("2000-10-10T10:00:00") AS a, date() AS b, time() AS c,
            datetime() AS d
      """
    Then the result should be, in any order:
      | a       | b                      | c                            | d                                               |
      | /^\d+$/ | /^\d{4}\-\d{2}-\d{2}$/ | /^\d{2}:\d{2}:\d{2}\.\d{6}$/ | /^\d{4}\-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}$/ |
    When executing query:
      """
      YIELD datetime('2019-03-02T22:00:30') as dt
      """
    Then the result should be, in any order:
      | dt                           |
      | '2019-03-02T22:00:30.000000' |
    When executing query:
      """
      YIELD datetime('2019-03-02 22:00:30') as dt
      """
    Then the result should be, in any order:
      | dt                           |
      | '2019-03-02T22:00:30.000000' |

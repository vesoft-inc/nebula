# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Time computation

  Background:
    Given a graph with space named "nba"

  Scenario Outline: date add or subtract duration
    When executing query:
      """
      WITH date('1984-10-11') as x, duration(<map>) as d
      RETURN x + d AS sum, x - d AS diff
      """
    Then the result should be, in any order:
      | sum   | diff   |
      | <sum> | <diff> |
    When executing query:
      """
      RETURN date('1984-10-11') + duration(<map>) AS sum, date('1984-10-11') - duration(<map>) AS diff
      """
    Then the result should be, in any order:
      | sum   | diff   |
      | <sum> | <diff> |

    Examples:
      | map                                                                   | sum          | diff         |
      | {years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70} | '1997-03-25' | '1972-04-27' |

  # TODO don't allow different sign
  # | {months: 1, days: -14, hours: 16, minutes: -12, seconds: 70}                                      | '1984-10-28' | '1984-09-25' |
  # TODO support decimal
  # | {years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5} | '1997-10-11' | '1971-10-12' |
  Scenario Outline: time add or subtract duration
    When executing query:
      """
      WITH time('12:31:14') as x, duration(<map>) as d
      RETURN x + d AS sum, x - d AS diff
      """
    Then the result should be, in any order:
      | sum   | diff   |
      | <sum> | <diff> |
    When executing query:
      """
      RETURN time('12:31:14') + duration(<map>) AS sum, time('12:31:14') - duration(<map>) AS diff
      """
    Then the result should be, in any order:
      | sum   | diff   |
      | <sum> | <diff> |

    Examples:
      | map                                                                   | sum               | diff              |
      | {years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70} | '04:44:24.000000' | '20:18:04.000000' |

  # TODO don't allow different sign
  # | {months: 1, days: -14, hours: 16, minutes: -12, seconds: 70}                                      | '04:20:24.000000001+01:00' | '20:42:04.000000001+01:00' |
  # TODO support decimal
  # | {years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5} | '22:29:27.500000004+01:00' | '02:33:00.499999998+01:00' |
  Scenario Outline: datetime add or subtract duration
    When executing query:
      """
      WITH datetime('1984-10-11T12:31:14') as x, duration(<map>) as d
      RETURN x + d AS sum, x - d AS diff
      """
    Then the result should be, in any order:
      | sum   | diff   |
      | <sum> | <diff> |
    When executing query:
      """
      WITH  as x,  as d
      RETURN datetime('1984-10-11T12:31:14') + duration(<map>) AS sum, datetime('1984-10-11T12:31:14') - duration(<map>) AS diff
      """
    Then the result should be, in any order:
      | sum   | diff   |
      | <sum> | <diff> |

    Examples:
      | map                                                                   | sum                          | diff                         |
      | {years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70} | '1997-03-26T04:44:24.000000' | '1972-04-26T20:18:04.000000' |

  # TODO don't allow different sign
  # | {months: 1, days: -14, hours: 16, minutes: -12, seconds: 70}                                      | '1984-10-29T04:20:24.000000001+01:00' | '1984-09-24T20:42:04.000000001+01:00' |
  # TODO support decimal
  # | {years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 3} | '1997-10-11T22:29:27.500000004+01:00' | '1971-10-12T02:33:00.499999998+01:00' |
  Scenario Outline: datetime add or subtract duration
    When executing query:
      """
      WITH datetime('1984-10-11T12:31:14') as x, duration(<map>) as d
      RETURN x + d AS sum, x - d AS diff
      """
    Then the result should be, in any order:
      | sum   | diff   |
      | <sum> | <diff> |
    When executing query:
      """
      RETURN datetime('1984-10-11T12:31:14') + duration(<map>) AS sum, datetime('1984-10-11T12:31:14') - duration(<map>) AS diff
      """
    Then the result should be, in any order:
      | sum   | diff   |
      | <sum> | <diff> |

    Examples:
      | map                                                                   | sum                          | diff                         |
      | {years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70} | '1997-03-26T04:44:24.000000' | '1972-04-26T20:18:04.000000' |

# TODO don't allow different sign
# | {months: 1, days: -14, hours: 16, minutes: -12, seconds: 70}                                      | '1984-10-29T04:20:24.000000001' | '1984-09-24T20:42:04.000000001' |
# TODO support decimal
# | {years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 3} | '1997-10-11T22:29:27.500000004' | '1971-10-12T02:33:00.499999998' |

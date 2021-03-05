# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: TypeConversion Expression

  Background:
    Given a graph with space named "nba"

  Scenario: toBoolean
    When executing query:
      """
      YIELD [toBoolean(true), toBoolean(false), toBoolean(1), toBoolean(3.14),
      toBoolean("trUe"), toBoolean("3.14"), toBoolean(null)] AS yield_toBoolean
      """
    Then the result should be, in any order:
      | yield_toBoolean                                     |
      | [true, false, BAD_TYPE, BAD_TYPE, true, NULL, NULL] |
    When executing query:
      """
      UNWIND [true, false, 1, 3.14, "trUe", "3.14", null] AS b
      RETURN toBoolean(b) AS unwind_toBoolean
      """
    Then the result should be, in any order:
      | unwind_toBoolean |
      | true             |
      | false            |
      | BAD_TYPE         |
      | BAD_TYPE         |
      | true             |
      | NULL             |
      | NULL             |

  Scenario: toFloat
    When executing query:
      """
      YIELD [toFloat(true), toFloat(false), toFloat(1), toFloat(3.14),
      toFloat("trUe"), toFloat("3.14"), toFloat(null)] AS yield_toFloat
      """
    Then the result should be, in any order:
      | yield_toFloat                                     |
      | [BAD_TYPE, BAD_TYPE, 1.0, 3.14, NULL, 3.14, NULL] |
    When executing query:
      """
      UNWIND [true, false, 1, 3.14, "trUe", "3.14", null] AS b
      RETURN toFloat(b) AS unwind_toFloat
      """
    Then the result should be, in any order:
      | unwind_toFloat |
      | BAD_TYPE       |
      | BAD_TYPE       |
      | 1.0            |
      | 3.14           |
      | NULL           |
      | 3.14           |
      | NULL           |

  Scenario: toInteger
    When executing query:
      """
      YIELD [toInteger(true), toInteger(false), toInteger(1), toInteger(3.14),
      toInteger("trUe"), toInteger("3.14"), toInteger(null)] AS yield_toInteger
      """
    Then the result should be, in any order:
      | yield_toInteger                           |
      | [BAD_TYPE, BAD_TYPE, 1, 3, NULL, 3, NULL] |
    When executing query:
      """
      UNWIND [true, false, 1, 3.14, "trUe", "3.14", null] AS b
      RETURN toInteger(b) AS unwind_toInteger
      """
    Then the result should be, in any order:
      | unwind_toInteger |
      | BAD_TYPE         |
      | BAD_TYPE         |
      | 1                |
      | 3                |
      | NULL             |
      | 3                |
      | NULL             |

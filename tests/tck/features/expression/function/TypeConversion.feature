# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: TypeConversion Expression

  Background:
    Given a graph with space named "nba"

  Scenario: toBoolean
    When executing query:
      """
      YIELD [toBoolean(true), toBoolean(false),
      toBoolean("trUe"), toBoolean("3.14"), toBoolean(null)] AS yield_toBoolean
      """
    Then the result should be, in any order:
      | yield_toBoolean                 |
      | [true, false, true, NULL, NULL] |
    When executing query:
      """
      UNWIND [true, false, "trUe", "3.14", null] AS b
      RETURN toBoolean(b) AS unwind_toBoolean
      """
    Then the result should be, in any order:
      | unwind_toBoolean |
      | true             |
      | false            |
      | true             |
      | NULL             |
      | NULL             |
    When executing query:
      """
      YIELD [toBoolean(1), toBoolean(3.14)] AS yield_toBoolean
      """
    Then a SemanticError should be raised at runtime: Type error `toBoolean(1)'

  Scenario: toFloat
    When executing query:
      """
      YIELD [toFloat(1), toFloat(3.14),
      toFloat("trUe"), toFloat("3.14"), toFloat(null)] AS yield_toFloat
      """
    Then the result should be, in any order:
      | yield_toFloat                 |
      | [1.0, 3.14, NULL, 3.14, NULL] |
    When executing query:
      """
      UNWIND [1, 3.14, "trUe", "3.14", null] AS b
      RETURN toFloat(b) AS unwind_toFloat
      """
    Then the result should be, in any order:
      | unwind_toFloat |
      | 1.0            |
      | 3.14           |
      | NULL           |
      | 3.14           |
      | NULL           |
    When executing query:
      """
      YIELD [toFloat(true), toFloat(false)] AS yield_toFloat
      """
    Then a SemanticError should be raised at runtime: Type error `toFloat(true)'

  Scenario: toInteger
    When executing query:
      """
      YIELD [toInteger(1), toInteger(3.14),
      toInteger("trUe"), toInteger("3.14"), toInteger(null), toInteger("1e3"),
      toInteger("1E3"), toInteger("1.5E4")] AS yield_toInteger
      """
    Then the result should be, in any order:
      | yield_toInteger                          |
      | [1, 3, NULL, 3, NULL, 1000, 1000, 15000] |
    When executing query:
      """
      UNWIND [1, 3.14, "trUe", "3.14", null] AS b
      RETURN toInteger(b) AS unwind_toInteger
      """
    Then the result should be, in any order:
      | unwind_toInteger |
      | 1                |
      | 3                |
      | NULL             |
      | 3                |
      | NULL             |
    When executing query:
      """
      YIELD [toInteger(true), toInteger(false)] AS yield_toInteger
      """
    Then a SemanticError should be raised at runtime: Type error `toInteger(true)'

  Scenario: toSet
    When executing query:
      """
      RETURN toSet(list[1,2,3,1,2]) AS list2set
      """
    Then the result should be, in any order:
      | list2set  |
      | {3, 1, 2} |
    When executing query:
      """
      RETURN toSet(set{1,2,3,1,2}) AS set2set
      """
    Then the result should be, in any order:
      | set2set   |
      | {3, 1, 2} |
    When executing query:
      """
      RETURN toSet(true) AS bool2set
      """
    Then a SemanticError should be raised at runtime: `toSet(true)' is not a valid expression : Parameter's type error
    When executing query:
      """
      RETURN toSet(1) AS int2set
      """
    Then a SemanticError should be raised at runtime: `toSet(1)' is not a valid expression : Parameter's type error
    When executing query:
      """
      RETURN toSet(3.4) AS float2set
      """
    Then a SemanticError should be raised at runtime: `toSet(3.4)' is not a valid expression : Parameter's type error

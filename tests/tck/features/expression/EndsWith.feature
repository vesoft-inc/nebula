# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Ends With Expression

  Background:
    Given a graph with space named "nba"

  Scenario: yield ends with
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'le'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "le") |
      | true                     |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "app") |
      | false                     |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "a") |
      | false                   |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'e'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "e") |
      | true                    |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'E'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "E") |
      | false                   |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "b") |
      | false                   |
    When executing query:
      """
      YIELD '123' ENDS WITH '3'
      """
    Then the result should be, in any order:
      | ("123" ENDS WITH "3") |
      | true                  |
    When executing query:
      """
      YIELD 123 ENDS WITH 3
      """
    Then the result should be, in any order:
      | (123 ENDS WITH 3) |
      | BAD_TYPE          |

  Scenario: yield not ends with
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'le'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "le") |
      | false                        |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "app") |
      | true                          |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "a") |
      | true                        |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'e'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "e") |
      | false                       |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'E'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "E") |
      | true                        |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "b") |
      | true                        |
    When executing query:
      """
      YIELD '123' NOT ENDS WITH '3'
      """
    Then the result should be, in any order:
      | ("123" NOT ENDS WITH "3") |
      | false                     |
    When executing query:
      """
      YIELD 123 NOT ENDS WITH 3
      """
    Then the result should be, in any order:
      | (123 NOT ENDS WITH 3) |
      | BAD_TYPE              |

  Scenario: ends with go
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst ENDS WITH 'Ginobili'
      YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |

  Scenario: not ends with go
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT ENDS WITH 'Ginobili'
      YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
      | 'Tony Parker'  |

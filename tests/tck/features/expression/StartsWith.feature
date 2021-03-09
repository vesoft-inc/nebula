# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Starts With Expression

  Background:
    Given a graph with space named "nba"

  Scenario: yield starts with
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "app") |
      | true                        |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "a") |
      | true                      |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'A'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "A") |
      | false                     |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "b") |
      | false                     |
    When executing query:
      """
      YIELD '123' STARTS WITH '1'
      """
    Then the result should be, in any order:
      | ("123" STARTS WITH "1") |
      | true                    |
    When executing query:
      """
      YIELD 123 STARTS WITH 1
      """
    Then the result should be, in any order:
      | (123 STARTS WITH 1) |
      | BAD_TYPE            |

  Scenario: yield not starts with
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "app") |
      | false                           |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "a") |
      | false                         |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'A'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "A") |
      | true                          |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "b") |
      | true                          |
    When executing query:
      """
      YIELD '123' NOT STARTS WITH '1'
      """
    Then the result should be, in any order:
      | ("123" NOT STARTS WITH "1") |
      | false                       |
    When executing query:
      """
      YIELD 123 NOT STARTS WITH 1
      """
    Then the result should be, in any order:
      | (123 NOT STARTS WITH 1) |
      | BAD_TYPE                |

  Scenario: starts with go
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst STARTS WITH 'LaMarcus'
      YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst STARTS WITH 'Obama'
      YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |

  Scenario: not starts with go
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT STARTS WITH 'T'
      YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
      | 'Tony Parker'  |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like
      WHERE like.likeness NOT IN [95,56,21] AND $$.player.name NOT STARTS WITH 'Tony'
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name      | $$.player.name | like.likeness |
      | 'Manu Ginobili'     | 'Tim Duncan'   | 90            |
      | 'LaMarcus Aldridge' | 'Tim Duncan'   | 75            |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like
      WHERE like.likeness NOT IN [95,56,21] AND $^.player.name NOT STARTS WITH 'LaMarcus'
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name  | $$.player.name | like.likeness |
      | 'Manu Ginobili' | 'Tim Duncan'   | 90            |

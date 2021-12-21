# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Sample and limit

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: Sample Limit Go in One step
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD edge as e LIMIT [-1]
      """
    Then a SemanticError should be raised at runtime: Limit/Sample element must be nonnegative.
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD $$ as dst LIMIT [1, 2]
      """
    Then a SemanticError should be raised at runtime: `[1,2]' length must be equal to GO step size 1
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst LIMIT [1]
      """
    Then the result should be, in any order:
      | like._dst             |
      | hash('Manu Ginobili') |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst LIMIT [3]
      """
    Then the result should be, in any order:
      | like._dst             |
      | hash('Manu Ginobili') |
      | hash('Tony Parker')   |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst SAMPLE [1]
      """
    Then the result should be, in any order:
      | like._dst |
      | /[\d\-+]/ |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst SAMPLE [3]
      """
    Then the result should be, in any order:
      | like._dst             |
      | hash('Manu Ginobili') |
      | hash('Tony Parker')   |

  Scenario: Sample Limit Go in Multiple steps
    When executing query:
      """
      GO 3 STEPS FROM hash('Tim Duncan') OVER like YIELD like._dst LIMIT [1, 2]
      """
    Then a SemanticError should be raised at runtime: `[1,2]' length must be equal to GO step size 3
    When executing query:
      """
      GO 3 STEPS FROM hash('Tim Duncan') OVER like YIELD like._dst LIMIT [1, 2, 3]
      """
    Then the result should be, in any order:
      | like._dst             |
      | hash('Manu Ginobili') |
      | hash('Tony Parker')   |

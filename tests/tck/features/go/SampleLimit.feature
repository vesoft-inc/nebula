# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Sample and limit

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: Sample Limit Go in One step
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD edge as e LIMIT [-1]
      """
    Then a SemanticError should be raised at runtime: Limit/Sample element must be nonnegative.
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like  YIELD $$ as dst LIMIT [1, 2]
      """
    Then a SemanticError should be raised at runtime: `[1,2]' length must be equal to GO step size 1.
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD $$ as dst LIMIT ["1"]
      """
    Then a SemanticError should be raised at runtime: Limit/Sample element type must be Integer.
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD edge as e SAMPLE ["1"]
      """
    Then a SemanticError should be raised at runtime: Limit/Sample element type must be Integer.
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst LIMIT [1]
      """
    Then the result should be, in any order:
      | like._dst       |
      | 'Manu Ginobili' |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst LIMIT [3]
      """
    Then the result should be, in any order:
      | like._dst       |
      | 'Manu Ginobili' |
      | 'Tony Parker'   |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst SAMPLE [1]
      """
    Then the result should be, in any order:
      | like._dst |
      | /[\s\w+]/ |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst SAMPLE [3]
      """
    Then the result should be, in any order:
      | like._dst       |
      | 'Manu Ginobili' |
      | 'Tony Parker'   |

  Scenario: Sample Limit Go in Multiple steps
    When executing query:
      """
      GO 3 STEPS FROM 'Tim Duncan' OVER like YIELD like._dst LIMIT [1, 2]
      """
    Then a SemanticError should be raised at runtime: `[1,2]' length must be equal to GO step size 3.
    When executing query:
      """
      GO 3 STEPS FROM 'Tim Duncan' OVER like YIELD like._dst LIMIT [1, 2, 3]
      """
    Then the result should be, in any order:
      | like._dst |
      | /[\s\w+]/ |
      | /[\s\w+]/ |
      | /[\s\w+]/ |

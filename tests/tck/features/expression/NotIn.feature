# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Not In Expression

  Scenario: yield NOT IN list
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD 1 NOT IN [1, 2, 3] AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    When executing query:
      """
      YIELD 0 NOT IN [1, 2, 3] AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD 'hello' NOT IN ['hello', 'world', 3] AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |

  Scenario: yield NOT IN set
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD 1 NOT IN {1, 2, 3} AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    When executing query:
      """
      YIELD 0 NOT IN {1, 2, 3} AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD 'hello' NOT IN {'hello', 'world', 3} AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |

  Scenario: Using NOT IN list in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN ['Danny Green']
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name      |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN ['Danny Green']
      """
    Then the result should be, in any order:
      | like._dst           |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness NOT IN [95,56,21]
      YIELD $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $$.player.name      | like.likeness |
      | 'LaMarcus Aldridge' | 90            |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name      | $$.player.name | like.likeness |
      | 'Manu Ginobili'     | 'Tim Duncan'   | 90            |
      | 'LaMarcus Aldridge' | 'Tim Duncan'   | 75            |
      | 'LaMarcus Aldridge' | 'Tony Parker'  | 75            |

  Scenario: Using NOT IN set in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN {'Danny Green'}
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name      |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN {'Danny Green'}
      """
    Then the result should be, in any order:
      | like._dst           |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness NOT IN {95,56,21}
      YIELD $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $$.player.name      | like.likeness |
      | 'LaMarcus Aldridge' | 90            |

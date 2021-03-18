# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: In Expression

  Scenario: yield IN list
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD 1 IN [1, 2, 3] AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD 0 IN [1, 2, 3] AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    When executing query:
      """
      YIELD 'hello' IN ['hello', 'world', 3, NULL] AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD 2 IN range(1, 3) AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD 2 IN [n IN range(1, 5) WHERE n > 3 | n - 2] AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |

  Scenario: yield IN set
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD 1 IN {1, 2, 3} AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD 0 IN {1, 2, 3, 1, 2} AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    When executing query:
      """
      YIELD 'hello' IN {'hello', 'world', 3} AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |

  Scenario: Using IN list in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst IN ['Tim Duncan', 'Danny Green']
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name |
      | "Tim Duncan"   |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst IN ['Danny Green']
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness IN [95,56,21]
      """
    Then the result should be, in any order:
      | like._dst       |
      | 'Tim Duncan'    |
      | 'Manu Ginobili' |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID |
      GO FROM $-.ID OVER like WHERE like.likeness IN [95,56,21]
      """
    Then the result should be, in any order:
      | like._dst       |
      | 'Tony Parker'   |
      | 'Manu Ginobili' |

  Scenario: Using IN set in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst IN {'Tim Duncan', 'Danny Green'}
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name |
      | "Tim Duncan"   |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst IN {'Danny Green'}
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness IN {95,56,21,95,90}
      YIELD $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $$.player.name      | like.likeness |
      | 'LaMarcus Aldridge' | 90            |
      | 'Manu Ginobili'     | 95            |
      | 'Tim Duncan'        | 95            |

  Scenario: Using IN list in MATCH
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)
      WHERE "Parker" IN split(v.name, " ")
      RETURN v.name
      """
    Then the result should be, in any order:
      | v.name        |
      | "Tony Parker" |
    When executing query:
      """
      MATCH (v:player)
      WHERE "ing" IN [n IN split(v.name, "M") WHERE true]
      RETURN v.name
      """
    Then the result should be, in any order:
      | v.name     |
      | "Yao Ming" |

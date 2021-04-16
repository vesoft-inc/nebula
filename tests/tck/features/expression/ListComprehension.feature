# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: ListComprehension

  Scenario: Returning a list comprehension
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD [n IN range(1, 5) WHERE n > 2 | n + 10] AS a
      """
    Then the result should be, in any order:
      | a            |
      | [13, 14, 15] |
    When executing query:
      """
      YIELD [n IN [1, 2, 3, 4, 5] WHERE n > 2] AS a
      """
    Then the result should be, in any order:
      | a         |
      | [3, 4, 5] |
    When executing query:
      """
      YIELD tail([n IN range(1, 5) | 2 * n - 10]) AS a
      """
    Then the result should be, in any order:
      | a               |
      | [-6, -4, -2, 0] |
    When executing query:
      """
      YIELD [n IN range(1, 3) WHERE true | n] AS r
      """
    Then the result should be, in any order:
      | r         |
      | [1, 2, 3] |

  Scenario: Using a list comprehension in a GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE like.likeness NOT IN [x IN [95, 100] | x + $$.player.age]
      YIELD like._dst AS id, like.likeness AS likeness
      """
    Then the result should be, in any order:
      | id                  | likeness |
      | "LaMarcus Aldridge" | 90       |
      | "Manu Ginobili"     | 95       |
      | "Tim Duncan"        | 95       |

  Scenario: Using a list comprehension in a MATCH
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m)
      RETURN [n IN nodes(p) WHERE n.name NOT STARTS WITH "LeBron" | n.age + 100] AS r
      """
    Then the result should be, in any order:
      | r     |
      | [134] |
      | [133] |
      | [131] |
      | [129] |
      | [137] |
      | [126] |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)
      RETURN [n IN nodes(p) | n.age + 100] AS r
      """
    Then the result should be, in any order:
      | r          |
      | [134, 143] |
    When executing query:
      """
      WITH [1, 2, 3, 4, 5, NULL] AS a, 1.3 AS b
      RETURN [n IN a WHERE n > 2 + b | n + a[0]] AS r1
      """
    Then the result should be, in any order:
      | r1     |
      | [5, 6] |

  Scenario: collection is not a LIST
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD [n IN 18 WHERE n > 2 | n + 10] AS a
      """
    Then a SemanticError should be raised at runtime: `18', expected LIST, but was INT
    When executing query:
      """
      YIELD [n IN NULL WHERE n > 2 | n + 10] AS a
      """
    Then the result should be, in any order:
      | a    |
      | NULL |

  Scenario: aggregate function in collection
    Given a graph with space named "nba"
    When executing query:
      """
      UNWIND [1, 2, 3, 4, 5] AS a RETURN a * 2 AS x
      | RETURN [n in collect($-.x) WHERE n > 5 | n + 1] AS list
      """
    Then the result should be, in any order:
      | list       |
      | [7, 9, 11] |

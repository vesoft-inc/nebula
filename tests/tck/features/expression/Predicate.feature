# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Predicate

  Scenario: yield a predicate
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD all(n IN range(1, 5) WHERE n > 2) AS r
      """
    Then the result should be, in any order:
      | r     |
      | False |
    When executing query:
      """
      YIELD any(n IN [1, 2, 3, 4, 5] WHERE n > 2) AS r
      """
    Then the result should be, in any order:
      | r    |
      | True |
    When executing query:
      """
      YIELD single(n IN range(1, 5) WHERE n == 3) AS r
      """
    Then the result should be, in any order:
      | r    |
      | True |
    When executing query:
      """
      YIELD none(n IN range(1, 3) WHERE n == 0) AS r
      """
    Then the result should be, in any order:
      | r    |
      | True |

  Scenario: use a predicate in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE any(x IN [5,  10] WHERE like.likeness + $$.player.age + x > 100)
      YIELD like._dst AS id, like.likeness AS likeness
      """
    Then the result should be, in any order:
      | id                  | likeness |
      | "LaMarcus Aldridge" | 90       |
      | "Manu Ginobili"     | 95       |
      | "Tim Duncan"        | 95       |

  Scenario: use a predicate in MATCH
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m)
      RETURN nodes(p)[0].name AS n1, nodes(p)[1].name AS n2,
      all(n IN nodes(p) WHERE n.name NOT STARTS WITH "D") AS b
      """
    Then the result should be, in any order:
      | n1             | n2                | b     |
      | "LeBron James" | "Carmelo Anthony" | True  |
      | "LeBron James" | "Chris Paul"      | True  |
      | "LeBron James" | "Danny Green"     | False |
      | "LeBron James" | "Dejounte Murray" | False |
      | "LeBron James" | "Dwyane Wade"     | False |
      | "LeBron James" | "Kyrie Irving"    | True  |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)
      RETURN single(n IN nodes(p) WHERE n.age > 40) AS b
      """
    Then the result should be, in any order:
      | b    |
      | True |
    When executing query:
      """
      WITH [1, 2, 3, 4, 5, NULL] AS a, 1.3 AS b
      RETURN any(n IN a WHERE n > 2 + b) AS r1
      """
    Then the result should be, in any order:
      | r1   |
      | True |

  Scenario: collection is not a LIST
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD single(n IN "Tom" WHERE n == 3) AS r
      """
    Then a SemanticError should be raised at runtime: `"Tom"', expected LIST, but was STRING
    When executing query:
      """
      YIELD single(n IN NULL WHERE n == 3) AS r
      """
    Then the result should be, in any order:
      | r    |
      | NULL |

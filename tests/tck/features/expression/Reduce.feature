# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Reduce

  Scenario: yield a reduce
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD reduce(totalNum = 10, n IN range(1, 3) | totalNum + n) AS r
      """
    Then the result should be, in any order:
      | r  |
      | 16 |
    When executing query:
      """
      YIELD reduce(totalNum = -4 * 5, n IN [1, 2] | totalNum + n * 2) AS r
      """
    Then the result should be, in any order:
      | r   |
      | -14 |

  Scenario: use a reduce in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE like.likeness != reduce(totalNum = 5, n IN range(1, 3) | $$.player.age + totalNum + n)
      YIELD like._dst AS id, $$.player.age AS age, like.likeness AS likeness
      """
    Then the result should be, in any order:
      | id                  | age | likeness |
      | "Manu Ginobili"     | 41  | 95       |
      | "Tim Duncan"        | 42  | 95       |
      | "LaMarcus Aldridge" | 33  | 90       |

  Scenario: use a reduce in MATCH
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m)
      RETURN
        nodes(p)[0].age AS age1,
        nodes(p)[1].age AS age2,
        reduce(totalAge = 100, n IN nodes(p) | totalAge + n.age) AS r
      """
    Then the result should be, in any order:
      | age1 | age2 | r   |
      | 34   | 34   | 168 |
      | 34   | 33   | 167 |
      | 34   | 31   | 165 |
      | 34   | 29   | 163 |
      | 34   | 37   | 171 |
      | 34   | 26   | 160 |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)
      RETURN nodes(p)[0].age AS age1,
             nodes(p)[1].age AS age2,
             reduce(x = 10, n IN nodes(p) | n.age - x) AS x
      """
    Then the result should be, in any order:
      | age1 | age2 | x  |
      | 34   | 43   | 19 |
    When executing query:
      """
      WITH [1, 2, NULL] AS a
      RETURN reduce(x = 10, n IN a | n - x) AS x
      """
    Then the result should be, in any order:
      | x    |
      | NULL |
    When executing query:
      """
      WITH [1, 2, 3] AS a, 2 AS b
      RETURN reduce(x = 10, n IN a | b*(n+x)) AS r1
      """
    Then the result should be, in any order:
      | r1  |
      | 102 |

  Scenario: collection is not a LIST
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD reduce(totalNum = 10, n IN "jerry" | totalNum + n) AS r
      """
    Then a SemanticError should be raised at runtime: `"jerry"', expected LIST, but was STRING
    When executing query:
      """
      YIELD reduce(totalNum = 10, n IN NULL | totalNum + n) AS r
      """
    Then the result should be, in any order:
      | r    |
      | NULL |

  Scenario: aggregate function in collection
    Given a graph with space named "nba"
    When executing query:
      """
      UNWIND [1, 2, 3, 4, 5] AS a RETURN a * 2 AS x
      | RETURN reduce(totalNum = 10, n in collect($-.x) | totalNum + n * 2) AS total
      """
    Then the result should be, in any order:
      | total |
      | 70    |

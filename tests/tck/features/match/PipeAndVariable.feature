# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Pipe or use variable to store the lookup results

  Background:
    Given a graph with space named "nba"

  @skip
  Scenario: pipe lookup results
    When executing query:
      """
      LOOKUP ON player
      WHERE player.name CONTAINS 'Tim'
      YIELD player.age AS age, id(vertex) AS vid |
      GO FROM $-.vid
      OVER like REVERSELY
      YIELD
        $-.age AS age,
        ('Tony Parker' == like._dst) AS liked,
        like._src AS src,
        like._dst AS dst
      """
    Then the result should be, in any order:
      | age | liked | src          | dst                 |
      | 42  | false | "Tim Duncan" | "Aron Baynes"       |
      | 42  | false | "Tim Duncan" | "Boris Diaw"        |
      | 42  | false | "Tim Duncan" | "Danny Green"       |
      | 42  | false | "Tim Duncan" | "Dejounte Murray"   |
      | 42  | false | "Tim Duncan" | "LaMarcus Aldridge" |
      | 42  | false | "Tim Duncan" | "Manu Ginobili"     |
      | 42  | false | "Tim Duncan" | "Marco Belinelli"   |
      | 42  | false | "Tim Duncan" | "Shaquille O'Neal"  |
      | 42  | false | "Tim Duncan" | "Tiago Splitter"    |
      | 42  | true  | "Tim Duncan" | "Tony Parker"       |

  @skip
  Scenario: use variable to store lookup results
    When executing query:
      """
      $var = LOOKUP ON player
      WHERE player.name CONTAINS 'Tim'
      YIELD player.age AS age, id(vertex) AS vid;
      GO FROM $var.vid
      OVER like REVERSELY
      YIELD
        $var.age AS age,
        ('Tony Parker' == like._dst) AS liked,
        like._src AS src,
        like._dst AS dst
      """
    Then the result should be, in any order:
      | age | liked | src          | dst                 |
      | 42  | false | "Tim Duncan" | "Aron Baynes"       |
      | 42  | false | "Tim Duncan" | "Boris Diaw"        |
      | 42  | false | "Tim Duncan" | "Danny Green"       |
      | 42  | false | "Tim Duncan" | "Dejounte Murray"   |
      | 42  | false | "Tim Duncan" | "LaMarcus Aldridge" |
      | 42  | false | "Tim Duncan" | "Manu Ginobili"     |
      | 42  | false | "Tim Duncan" | "Marco Belinelli"   |
      | 42  | false | "Tim Duncan" | "Shaquille O'Neal"  |
      | 42  | false | "Tim Duncan" | "Tiago Splitter"    |
      | 42  | true  | "Tim Duncan" | "Tony Parker"       |

  @skip
  Scenario: yield collect
    When executing query:
      """
      LOOKUP ON player
      WHERE player.name CONTAINS 'Tim'
      YIELD player.age as age, id(vertex) as vid |
      GO FROM $-.vid OVER like REVERSELY YIELD $-.age AS age, like._dst AS dst |
      YIELD
        any(d IN COLLECT(DISTINCT $-.dst) WHERE d=='Tony Parker') AS d,
        $-.age as age
      """
    Then the result should be, in any order:
      | d    | age |
      | true | 42  |

  Scenario: mixed usage of cypher and ngql
    When executing query:
      """
      LOOKUP ON player
      WHERE player.name == 'Tim Duncan'
      YIELD player.age as age, id(vertex) as vid
      | UNWIND $-.vid as a RETURN a
      """
    Then a SyntaxError should be raised at runtime: syntax error near `UNWIND'
    When executing query:
      """
      GET SUBGRAPH 2 STEPS FROM "Tim Duncan" BOTH like YIELD edges as e
      | UNWIND $-.e as a RETURN a
      """
    Then a SyntaxError should be raised at runtime: syntax error near `UNWIND'
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Yao Ming" OVER like YIELD path as p
      | UNWIND $-.p as a RETURN a
      """
    Then a SyntaxError should be raised at runtime: syntax error near `UNWIND'
    When executing query:
      """
      GO 2 STEPS FROM "Tim Duncan" OVER * YIELD dst(edge) as id
      | MATCH (v:player {name: "Yao Ming"}) WHERE id(v) == $-.id RETURN v
      """
    Then a SyntaxError should be raised at runtime: syntax error near `MATCH'
    When executing query:
      """
      MATCH (v:player) WHERE id(v) == "Tim Duncan" RETURN id(v) as id
      | GO 2 STEPS FROM $-.id OVER * YIELD dst(edge) as id
      """
    Then a SyntaxError should be raised at runtime: syntax error near `| GO 2 S'
    When executing query:
      """
      MATCH (v:player{name : "Tim Duncan"})-[e:like*2..3]-(b:player) RETURN id(b) as id
      | GO 1 TO 2 STEPS FROM $-.id OVER * YIELD dst(edge) as id
      """
    Then a SyntaxError should be raised at runtime: syntax error near `| GO 1 T'
    When executing query:
      """
      $var = MATCH (v:player{name : "Tim Duncan"})-[e:like*2..3]-(b:player) RETURN id(b) as id
      GO 1 TO 2 STEPS FROM $var.id OVER * YIELD dst(edge) as id
      """
    Then a SyntaxError should be raised at runtime: syntax error near `MATCH'

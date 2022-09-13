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
      YIELD ['Tim Duncan', 'Tony Parker'] AS a
      | UNWIND $-.a AS b
      | GO FROM $-.b OVER like YIELD edge AS e
      """
    Then the result should be, in any order:
      | e                                                            |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    When executing query:
      """
      YIELD {a:1, b:['Tim Duncan', 'Tony Parker'], c:'Tim Duncan'} AS a
      | YIELD $-.a.b AS b
      | UNWIND $-.b AS c
      | GO FROM $-.c OVER like YIELD edge AS e
      """
    Then the result should be, in any order:
      | e                                                            |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    When executing query:
      """
      YIELD {a:1, b:['Tim Duncan', 'Tony Parker'], c:'Tim Duncan'} AS a
      | YIELD $-.a.c AS b
      | UNWIND $-.b AS c
      | GO FROM $-.c OVER like YIELD edge AS e
      """
    Then the result should be, in any order:
      | e                                                       |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    When executing query:
      """
      LOOKUP ON player
      WHERE player.name == 'Tim Duncan'
      YIELD player.age as age, id(vertex) as vid
      | UNWIND $-.vid as a | YIELD $-.a AS a
      """
    Then the result should be, in any order, with relax comparison:
      | a            |
      | "Tim Duncan" |
    When executing query:
      """
      GET SUBGRAPH 2 STEPS FROM "Tim Duncan" BOTH like YIELD edges as e
      | UNWIND $-.e as a | YIELD $-.a AS a
      """
    Then the result should be, in any order, with relax comparison:
      | a                                                    |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {}]            |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {}]             |
      | [:like "Danny Green"->"Tim Duncan" @0 {}]            |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {}]        |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {}]      |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {}]          |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {}]        |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {}]       |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {}]         |
      | [:like "Tony Parker"->"Tim Duncan" @0 {}]            |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {}]          |
      | [:like "Tim Duncan"->"Tony Parker" @0 {}]            |
      | [:like "Damian Lillard"->"LaMarcus Aldridge" @0 {}]  |
      | [:like "Rudy Gay"->"LaMarcus Aldridge" @0 {}]        |
      | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {}]     |
      | [:like "Boris Diaw"->"Tony Parker" @0 {}]            |
      | [:like "Dejounte Murray"->"Chris Paul" @0 {}]        |
      | [:like "Dejounte Murray"->"Danny Green" @0 {}]       |
      | [:like "Dejounte Murray"->"James Harden" @0 {}]      |
      | [:like "Dejounte Murray"->"Kevin Durant" @0 {}]      |
      | [:like "Dejounte Murray"->"Kyle Anderson" @0 {}]     |
      | [:like "Dejounte Murray"->"LeBron James" @0 {}]      |
      | [:like "Dejounte Murray"->"Manu Ginobili" @0 {}]     |
      | [:like "Dejounte Murray"->"Marco Belinelli" @0 {}]   |
      | [:like "Dejounte Murray"->"Russell Westbrook" @0 {}] |
      | [:like "Dejounte Murray"->"Tony Parker" @0 {}]       |
      | [:like "Danny Green"->"LeBron James" @0 {}]          |
      | [:like "Danny Green"->"Marco Belinelli" @0 {}]       |
      | [:like "Marco Belinelli"->"Danny Green" @0 {}]       |
      | [:like "Marco Belinelli"->"Tony Parker" @0 {}]       |
      | [:like "Yao Ming"->"Shaquille O'Neal" @0 {}]         |
      | [:like "Shaquille O'Neal"->"JaVale McGee" @0 {}]     |
      | [:like "Tiago Splitter"->"Manu Ginobili" @0 {}]      |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {}]     |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {}]         |
      | [:like "James Harden"->"Russell Westbrook" @0 {}]    |
      | [:like "Chris Paul"->"LeBron James" @0 {}]           |
      | [:like "Russell Westbrook"->"James Harden" @0 {}]    |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER like YIELD path as p
      | YIELD nodes($-.p) AS nodes | UNWIND $-.nodes AS a | YIELD $-.a AS a
      """
    Then the result should be, in any order, with relax comparison:
      | a               |
      | ("Tim Duncan")  |
      | ("Tony Parker") |
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

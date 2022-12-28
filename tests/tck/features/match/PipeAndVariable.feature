# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Pipe/Variable

  Background:
    Given a graph with space named "nba"

  Scenario: Variable
    When executing query:
      """
      $v1 = GO FROM "Tony Parker" OVER like YIELD id($$) AS dst, $^.player.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 | age |
      | "LaMarcus Aldridge" | 36  |
      | "Manu Ginobili"     | 36  |
      | "Tim Duncan"        | 36  |
    When executing query:
      """
      $v1 = GO FROM "Tony Parker" OVER like YIELD id($$) AS dst, $^.player.age AS age;
      YIELD $v1
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `$v1'
    When executing query:
      """
      $v1 = GO FROM "Tony Parker" OVER like YIELD id($$) AS dst, $^.player.age AS age;
      YIELD $v1.age
      """
    Then the result should be, in any order, with relax comparison:
      | $v1.age |
      | 36      |
      | 36      |
      | 36      |
    When executing query:
      """
      $v1 = FETCH PROP ON player "Tony Parker"
      YIELD properties(Vertex) AS props, properties(Vertex).name AS name, properties(Vertex).age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | props                          | name          | age |
      | {age: 36, name: "Tony Parker"} | "Tony Parker" | 36  |
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      $v2 = YIELD "Tim Duncan" AS b;
      GO FROM $v1.a OVER like WHERE $v1.a > $v2.b YIELD id($$) AS dst;
      """
    Then a SemanticError should be raised at runtime: Multiple variable property is not supported in WHERE or YIELD
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      GO FROM "Tim Duncan" OVER like WHERE id($$) != $v1.a
      YIELD id($$) AS dst
      """
    Then a SemanticError should be raised at runtime: A variable must be referred in FROM before used in WHERE or YIELD
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      GO FROM "Tim Duncan" OVER like WHERE id($$) != $v1.a
      YIELD id($$) AS dst
      """
    Then a SemanticError should be raised at runtime: A variable must be referred in FROM before used in WHERE or YIELD
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      GO FROM "Tim Duncan" OVER like
      YIELD id($$) AS dst, $v1.a AS dst2
      """
    Then a SemanticError should be raised at runtime: A variable must be referred in FROM before used in WHERE or YIELD
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      GO FROM $v1.a OVER like WHERE id($$) != $v1.a
      YIELD id($$) AS dst, $v1.a AS dst2
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 | dst2          |
      | "LaMarcus Aldridge" | "Tony Parker" |
      | "Manu Ginobili"     | "Tony Parker" |
      | "Tim Duncan"        | "Tony Parker" |
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      $v1 = YIELD "Tim Duncan" AS a;
      """
    Then a SemanticError should be raised at runtime: Variable `v1' already exists
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      $v2 = YIELD $v1.a AS a;
      """
    Then the result should be, in any order, with relax comparison:
      | a             |
      | "Tony Parker" |
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      $v2 = YIELD $v1.a AS a;
      YIELD $v1.a AS a UNION ALL YIELD $v2.a AS a
      """
    Then the result should be, in any order, with relax comparison:
      | a             |
      | "Tony Parker" |
      | "Tony Parker" |
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      $v2 = YIELD $v1.a AS a;
      YIELD $v1.a, $v2.a
      """
    Then a SemanticError should be raised at runtime: Only one variable allowed to use.
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      FETCH PROP ON player $v1.a YIELD player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name          |
      | "Tony Parker" |
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      FETCH PROP ON player $v1.a YIELD player.name AS name, $v1.a AS a
      """
    Then a SemanticError should be raised at runtime: unsupported input/variable property expression in yield.
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      LOOKUP ON player WHERE player.name == $v1.a YIELD player.name AS name, $v1.a
      """
    Then a SemanticError should be raised at runtime: '$v1.a' is not an evaluable expression.
    When executing query:
      """
      $v1 = YIELD "Tony Parker" AS a;
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD player.name AS name, $v1.a
      """
    Then a SemanticError should be raised at runtime: unsupported input/variable property expression in yield.

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

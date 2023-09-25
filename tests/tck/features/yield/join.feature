# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: join

  Background:
    Given a graph with space named "nba"

  Scenario: invalid join
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e;
      YIELD $a.vid AS id, $b.e AS e, count(*) FROM $a INNER JOIN $b ON $a.vid == $b.vid
      """
    Then a SyntaxError should be raised at runtime:  Invalid use of aggregating function in yield clause. near `$a.vid AS id, $b.e AS e, count(*)'
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e;
      YIELD $a.vid AS id, $b.e AS e FROM $a INNER JOIN $b ON $a.vid == $b.vid
      """
    Then a SemanticError should be raised at runtime:  column name `e' of $a and column name `e' of $b are the same, please rename it to a non-duplicate column name.
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2;
      YIELD $a.vid AS id, $b.e2 AS e FROM $a LEFT JOIN $b ON $a.vid == $b.vid
      """
    Then a SemanticError should be raised at runtime:  only support inner join.
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2;
      YIELD $a.vid AS id, $b.e2 AS e FROM $a RIGHT JOIN $b ON $a.vid == $b.vid
      """
    Then a SemanticError should be raised at runtime:  only support inner join.
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2;
      YIELD $a.vid AS id, $b.e2 AS e FROM $a OUTER JOIN $b ON $a.vid == $b.vid
      """
    Then a SemanticError should be raised at runtime:  only support inner join.
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2;
      YIELD $a.vid AS id, $b.e2 AS e FROM $a SEMI JOIN $b ON $a.vid == $b.vid
      """
    Then a SemanticError should be raised at runtime:  only support inner join.
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2;
      YIELD $a.vid AS id, $b.e2 AS e FROM $a INNER JOIN $a ON $a.vid == $a.vid
      """
    Then a SemanticError should be raised at runtime:  do not support self-join.
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2;
      YIELD $a.vid AS id, $b.e2 AS e FROM $a INNER JOIN $b ON $a.vid == $a.vid
      """
    Then a SemanticError should be raised at runtime: `b' should be consistent with join condition variable `$a.vid'.
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2;
      YIELD $a.vid AS id, $b.e2 AS e FROM $a INNER JOIN $b ON $a.vid == $b.noexist
      """
    Then a SemanticError should be raised at runtime: `$b.noexist', not exist prop `noexist'
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e;
      GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2
      | YIELD $a.vid AS id, $-.e2 AS e FROM $a INNER JOIN $- ON $a.vid == $-.vid
      """
    Then a SyntaxError should be raised at runtime: syntax error near `$-'
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as a;
      $b = YIELD $a.a from $a inner join $b on $a.a == $b.a
      """
    Then a SemanticError should be raised at runtime: variable: `b' not exist
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as a;
      YIELD $a.a from $a inner join $b on $a.a == $b.a
      """
    Then a SemanticError should be raised at runtime: variable: `b' not exist

  Scenario: join go
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD id($$) as vid, edge as e
      | GO FROM $-.vid OVER like YIELD id($$) as vid, $-.e AS e1, edge as e2
      """
    Then the result should be, in any order, with relax comparison:
      | vid                 | e1                                                      | e2                                                           |
      | "Tim Duncan"        | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "LaMarcus Aldridge" | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | "Manu Ginobili"     | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | "Tim Duncan"        | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as dst, edge as e;
      $b = GO FROM $a.dst OVER like YIELD id($^) as src, id($$) as vid, edge AS e2;
      YIELD $b.vid AS vid, $a.e AS e1, $b.e2 AS e2 FROM $a INNER JOIN $b ON $a.dst == $b.src
      """
    Then the result should be, in any order, with relax comparison:
      | vid                 | e1                                                      | e2                                                           |
      | "Tim Duncan"        | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "LaMarcus Aldridge" | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | "Manu Ginobili"     | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | "Tim Duncan"        | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as dst, edge as e;
      $b = GO FROM 'Tony Parker', 'Manu Ginobili' OVER like YIELD id($^) as src, id($$) as vid, edge AS e2;
      YIELD $b.vid AS vid, $a.e AS e1, $b.e2 AS e2 FROM $a INNER JOIN $b ON $a.dst == $b.src
      """
    Then the result should be, in any order, with relax comparison:
      | vid                 | e1                                                      | e2                                                           |
      | "Tim Duncan"        | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "LaMarcus Aldridge" | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | "Manu Ginobili"     | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | "Tim Duncan"        | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as dst, edge as e;
      $b = GO FROM $a.dst OVER like YIELD id($^) as src, id($$) as vid, edge AS e2;
      YIELD $b.vid AS vid, $a.e AS e1, $b.e2 AS e2 FROM $a INNER JOIN $b ON $a.dst == $b.src
      | GO FROM $-.vid OVER like YIELD $-.vid AS src, $$ AS dst, $-.e1 AS e
      """
    Then the result should be, in any order, with relax comparison:
      | src                 | dst                                                                                                         | e                                                       |
      | "LaMarcus Aldridge" | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "LaMarcus Aldridge" | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "Tim Duncan"        | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | "Tim Duncan"        | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "Tim Duncan"        | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | "Tim Duncan"        | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "Manu Ginobili"     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD id($$) as dst, edge as e;
      $b = GO FROM $a.dst OVER like YIELD id($^) as src, id($$) as vid, edge AS e2;
      $c = YIELD $b.vid AS vid, $a.e AS e1, $b.e2 AS e2 FROM $a INNER JOIN $b ON $a.dst == $b.src;
      GO FROM $c.vid OVER like YIELD $c.vid AS src, $$ AS dst, $c.e1 AS e
      """
    Then the result should be, in any order, with relax comparison:
      | src                 | dst                                                                                                         | e                                                       |
      | "LaMarcus Aldridge" | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "LaMarcus Aldridge" | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "Tim Duncan"        | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | "Tim Duncan"        | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "Tim Duncan"        | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | "Tim Duncan"        | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "Manu Ginobili"     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |

  Scenario: join lookup
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.name == 'Tony Parker' YIELD id(vertex) as dst, vertex AS v;
      $b = GO FROM 'Tony Parker', 'Manu Ginobili' OVER like YIELD id($^) as src, id($$) as vid, edge AS e2;
      YIELD $b.vid AS vid, $a.v AS v, $b.e2 AS e2 FROM $a INNER JOIN $b ON $a.dst == $b.src
      """
    Then the result should be, in any order, with relax comparison:
      | vid                 | v                                                     | e2                                                           |
      | "LaMarcus Aldridge" | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | "Manu Ginobili"     | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | "Tim Duncan"        | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.name == 'Tony Parker' YIELD id(vertex) as dst, vertex AS v;
      $b = LOOKUP on player WHERE player.age > 30 YIELD id(vertex) AS src, vertex AS v2;
      YIELD $b.src AS vid, $a.v AS v, $b.v2 AS v2 FROM $a INNER JOIN $b ON $a.dst == $b.src
      """
    Then the result should be, in any order, with relax comparison:
      | vid           | v                                                     | v2                                                    |
      | "Tony Parker" | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |

  Scenario: join fetch
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.name == 'Tony Parker' YIELD id(vertex) as src, vertex AS v;
      $b = FETCH PROP ON like 'Tony Parker'->'Tim Duncan' YIELD src(edge) as src, edge as e;
      YIELD $a.src AS src, $a.v AS v, $b.e AS e FROM $a INNER JOIN $b ON $a.src == $b.src
      """
    Then the result should be, in any order, with relax comparison:
      | src           | v                                                     | e                                                     |
      | "Tony Parker" | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}] |

  Scenario: join path
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.name == 'Tony Parker' YIELD id(vertex) as src, vertex AS v;
      $b = (FIND SHORTEST PATH FROM $a.src TO 'Tim Duncan' OVER like YIELD path AS p | YIELD $-.p AS p, id(startNode($-.p)) AS src);
      YIELD $a.src AS src, $a.v AS v, $b.p AS p FROM $a INNER JOIN $b ON $a.src == $b.src
      """
    Then the result should be, in any order, with relax comparison:
      | src           | v                                                     | p                                              |
      | "Tony Parker" | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")> |
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.name == 'Tony Parker' YIELD id(vertex) as src, vertex AS v;
      $b = GO 2 TO 5 STEPS FROM $a.src OVER like WHERE $$.player.age > 30 YIELD id($$) AS dst;
      $c = (FIND ALL PATH FROM $a.src TO $b.dst OVER like YIELD path AS p | YIELD $-.p AS forward, id(endNode($-.p)) AS dst);
      $d = (FIND SHORTEST PATH FROM $c.dst TO $a.src OVER like YIELD path AS p | YIELD $-.p AS p, id(startNode($-.p)) AS src);
      YIELD $c.forward AS forwardPath, $c.dst AS end, $d.p AS backwordPath  FROM $c INNER JOIN $d ON $c.dst == $d.src
      """
    Then the result should be, in any order, with relax comparison:
      | forwardPath                                                                                                                                                                      | end                 | backwordPath                                                                        |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")>                                                                                                                            | "LaMarcus Aldridge" | <("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                               |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")>                                                                 | "LaMarcus Aldridge" | <("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                               |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")>                                                                                                                                   | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")>                                                                                               | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>                                                                                                   | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")>                                                                 | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>                                                                      | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                                                                                              | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                                                                                              | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                                                                                     | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                                                                                     | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                                                 | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                                                 | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                                                     | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                                                     | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")>                                                                                                                                | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")>                                                                                                   | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")>                                                               | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")>                                                              | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")>                                                                   | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")>                                                                     | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")>                                 | "LaMarcus Aldridge" | <("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                               |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")>                                    | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>                                  | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>                                 | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>                                        | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")>                                    | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")>                                        | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                                   | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                                   | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                   | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                   | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                        | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                        | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")>                                 | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")>                                 | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")>    | "LaMarcus Aldridge" | <("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                               |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")>    | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>    | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>    | "Tim Duncan"        | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                      |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>   | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>   | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>    | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>    | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>   | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>   | "Tony Parker"       | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>        |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")>    | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")> | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")>        | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")>    | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")>        | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")>   | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")>   | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |
      | <("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")>        | "Manu Ginobili"     | <("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>      |

  Scenario: multiple join
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.name == 'Tony Parker' YIELD id(vertex) as dst, vertex AS v;
      $b = GO FROM 'Tony Parker', 'Manu Ginobili' OVER like YIELD id($^) as src, id($$) as vid, edge AS e2;
      $c = YIELD $b.vid AS vid, $a.v AS v, $b.e2 AS e2 FROM $a INNER JOIN $b ON $a.dst == $b.src;
      $d = GO 4 STEPS FROM $b.vid OVER like BIDIRECT YIELD id($$) as vid, edge AS e3;
      YIELD $d.e3 AS e, $c.e2 AS e2 FROM $c INNER JOIN $d ON $c.vid == $d.vid
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                               | e2                                                           |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]         | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]         | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]         | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]         | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]     | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]     | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]     | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]     | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]        | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]        | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]        | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]        | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}]    | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}]    | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}]    | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}]    | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]       | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]       | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]       | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]       | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]    | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]    | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]    | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]    | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]    | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]    | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]    | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]    | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]        | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]        | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]        | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]        | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]           | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]         | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]         | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]         | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]         | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Rudy Gay"->"LaMarcus Aldridge" @0 {likeness: 70}]       | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Rudy Gay"->"LaMarcus Aldridge" @0 {likeness: 70}]       | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Rudy Gay"->"LaMarcus Aldridge" @0 {likeness: 70}]       | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Rudy Gay"->"LaMarcus Aldridge" @0 {likeness: 70}]       | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]            | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]            | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]            | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]            | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Damian Lillard"->"LaMarcus Aldridge" @0 {likeness: 80}] | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Damian Lillard"->"LaMarcus Aldridge" @0 {likeness: 80}] | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Damian Lillard"->"LaMarcus Aldridge" @0 {likeness: 80}] | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Damian Lillard"->"LaMarcus Aldridge" @0 {likeness: 80}] | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]       | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]       | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]       | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]       | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |

  Scenario: join limit
    When executing query:
      """
      $a = GO FROM "Tim Duncan" OVER like YIELD src(edge) AS src, edge AS e1;
      $b = GO 2 STEPS FROM "Tony Parker" OVER like YIELD edge AS e2, dst(edge) AS dst;
      YIELD $a.e1 AS e1, $b.e2 AS e2 from $a inner join $b ON $a.src == $b.dst | limit 10
      """
    Then the result should be, in any order, with relax comparison:
      | e1                                                      | e2                                                          |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     |

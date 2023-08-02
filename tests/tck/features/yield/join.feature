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
      $b = GO FROM 'Tony Parker' OVER like YIELD id($$) as vid, edge as e2;
      YIELD $a.vid AS id, $b.e2 AS e FROM $a LEFT JOIN $b ON $a.vid == $b.vid
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

  Scenario: join lookup
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.name == 'Tony Parker' YIELD id(vertex) as dst, vertex AS v;
      $b = GO FROM 'Tony Parker', 'Manu Ginobili' OVER like YIELD id($^) as src, id($$) as vid, edge AS e2;
      YIELD $b.vid AS vid, $a.v AS v, $b.e2 AS e2 FROM $a INNER JOIN $b ON $a.dst == $b.src
      """
    Then the result should be, in any order, with relax comparison:
      | vid                 | e1                                                    | e2                                                           |
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

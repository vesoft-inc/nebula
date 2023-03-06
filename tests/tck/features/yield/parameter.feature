# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License
Feature: Parameter

  Background:
    Given a graph with space named "nba"
    Given parameters: {"p1":1,"p2":true,"p3":"Tim Duncan","p4":3.3,"p5":[1,true,3],"p6":{"a":3,"b":false,"c":"Tim Duncan"},"p7":{"a":{"b":{"c":"Tim Duncan","d":[1,2,3,true,"Tim Duncan"]}}},"p8":"Manu Ginobili", "p9":["Tim Duncan","Tony Parker"]}

  Scenario: [param-test-001] without define param
    When executing query:
      """
      RETURN $p_not_exist AS v
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `$p_not_exist'

  Scenario: [param-test-002] support null
    When executing query:
      """
      RETURN $p1 is not null AS v
      """
    Then the result should be, in any order:
      | v    |
      | true |
    When executing query:
      """
      RETURN $p1 is null AS v
      """
    Then the result should be, in any order:
      | v     |
      | false |

  Scenario: [param-test-003] return parameters
    When executing query:
      """
      RETURN abs($p1)+1 AS ival, $p2 and false AS bval, $p3+"ef" AS sval, round($p4)+1.1 AS fval, $p5 AS lval, $p6.a AS mval, all(item in $p7.a.b.d where item<4 or ((item>0) is null)) AS pval
      """
    Then the result should be, in any order:
      | ival | bval  | sval           | fval | lval       | mval | pval |
      | 2    | false | "Tim Duncanef" | 4.1  | [1,true,3] | 3    | true |
    # return map
    When executing query:
      """
      RETURN $p6 AS v
      """
    Then the result should be, in any order:
      | v                                 |
      | {a: 3, b: false, c: "Tim Duncan"} |

  Scenario: [param-test-004] cypher with parameters
    # where clause
    When executing query:
      """
      MATCH (v) WHERE id(v)==$p3
      RETURN id(v) AS v
      """
    Then the result should be, in any order:
      | v            |
      | "Tim Duncan" |
    When executing query:
      """
      MATCH (v) WHERE id(v) IN [$p3,$p8]
      RETURN id(v) AS v
      """
    Then the result should be, in any order:
      | v               |
      | "Tim Duncan"    |
      | "Manu Ginobili" |
    When executing query:
      """
      MATCH (v) WHERE id(v) == $p7.a.b.d[4]
      RETURN id(v) AS v
      """
    Then the result should be, in any order:
      | v            |
      | "Tim Duncan" |
    When executing query:
      """
      MATCH (v) WHERE id(v) IN $p7.a.b.d
      RETURN id(v) AS v
      """
    Then the result should be, in any order:
      | v            |
      | "Tim Duncan" |
    When profiling query:
      """
      MATCH (v) WHERE id(v) IN $p9
      RETURN v.player.name AS v
      """
    Then the result should be, in any order:
      | v             |
      | "Tim Duncan"  |
      | "Tony Parker" |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 9  | Project        | 7            |               |
      | 7  | Filter         | 2            |               |
      | 2  | AppendVertices | 6            |               |
      | 6  | Dedup          | 6            |               |
      | 6  | PassThrough    | 0            |               |
      | 0  | Start          |              |               |
    # LIMIT
    When executing query:
      """
      MATCH (v:player)-[:like]->(n) WHERE id(v)==$p3 and n.player.age>$p1+29
      RETURN n.player.name AS dst LIMIT $p1+1
      """
    Then the result should be, in any order:
      | dst             |
      | "Manu Ginobili" |
      | "Tony Parker"   |
    When executing query:
      """
      MATCH (v:player)-[:like]->(n:player{name:$p7.a.b.c})
      RETURN n.player.name AS dst LIMIT $p7.a.b.d[0]
      """
    Then the result should be, in any order:
      | dst          |
      | "Tim Duncan" |
    # WITH clause
    When executing query:
      """
      UNWIND $p5 AS c
      WITH $p6.a AS a, $p6.b AS b, c
      RETURN a,b,c
      """
    Then the result should be, in any order:
      | a | b     | c    |
      | 3 | false | 1    |
      | 3 | false | true |
      | 3 | false | 3    |
    When executing query:
      """
      UNWIND abs($p1)+1 AS ival
      WITH ival AS ival, $p2 and false AS bval, $p3+"ef" AS sval, round($p4)+1.1 AS fval
      RETURN *
      """
    Then the result should be, in any order:
      | ival | bval  | sval           | fval |
      | 2    | false | "Tim Duncanef" | 4.1  |
    # order by
    When executing query:
      """
      MATCH (v:player)
      WITH v AS v WHERE v.player.name in [$p1,$p2,$p3,"Tony Parker",$p4,$p5,$p6]
      RETURN v.player.name AS v ORDER BY v, $p3 LIMIT $p1
      """
    Then the result should be, in order:
      | v            |
      | "Tim Duncan" |

  Scenario: [param-test-005] lookup with parameters
    When executing query:
      """
      LOOKUP ON player where player.age>$p1+40 YIELD player.name AS name
      """
    Then the result should be, in any order:
      | name               |
      | "Grant Hill"       |
      | "Jason Kidd"       |
      | "Vince Carter"     |
      | "Tim Duncan"       |
      | "Shaquille O'Neal" |
      | "Steve Nash"       |
      | "Ray Allen"        |

  Scenario: [param-test-006] go with parameters
    # yield clause
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like yield like._dst as dst, $p3;
      """
    Then the result should be, in order:
      | dst             | $p3          |
      | "Manu Ginobili" | "Tim Duncan" |
      | "Tony Parker"   | "Tim Duncan" |
    # where clause
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like WHERE like.likeness>$p1 yield like._dst as dst;
      """
    Then the result should be, in order:
      | dst             |
      | "Manu Ginobili" |
      | "Tony Parker"   |
    # step cannot support
    When executing query:
      """
      GO $1 STEPS FROM "Tim Duncan" OVER like yield like._dst as dst;
      """
    Then a SyntaxError should be raised at runtime: syntax error near `$1 STEPS'
    # from cannot support
    When executing query:
      """
      GO FROM $p3 OVER like yield like._dst as dst;
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'
    When executing query:
      """
      $var=GO FROM $p4 OVER like WHERE like.likeness>$p1 yield like._dst as dst;
      GO FROM $p1.dst OVER like YIELD DISTINCT $$.player.name AS name
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p4'
    When executing query:
      """
      GO FROM $p3,$p4 OVER like
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'
    When executing query:
      """
      GO 1 TO 2 STEPS FROM "Tim Duncan" OVER like YIELD like._dst AS dst SAMPLE [1,$p1]
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in sample clause near `[1,$p1]'

  Scenario: [param-test-007] fetch with parameters
    When executing query:
      """
      FETCH PROP ON player $nonexist
      """
    Then a SyntaxError should be raised at runtime: Variable is not supported in vid near `$nonexist'
    When executing query:
      """
      FETCH PROP ON player $p3,$p4
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'

  Scenario: [param-test-008] find-path with parameters
    When executing query:
      """
      find noloop path from $p3 to $p2 over like
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'
    When executing query:
      """
      find all path from $p3 to $p2 over like
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'
    When executing query:
      """
      find shortest path from $p3 to $p2 over like
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'

  Scenario: [param-test-009] get-subgraph with parameters
    When executing query:
      """
      GET SUBGRAPH FROM $p3 BOTH like
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'

  Scenario: [param-test-010] error type
    When executing query:
      """
      LOOKUP ON player WHERE player.age>$p2+43
      """
    Then a SemanticError should be raised at runtime: Type error `(true+43)'
    When executing query:
      """
      MATCH (v:player) RETURN  v LIMIT $p6
      """
    Then a SemanticError should be raised at runtime: LIMIT should be of type integer
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      MATCH (v:player) WHERE $var RETURN  v,$var LIMIT $p6
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `$var'
    Then clear the used parameters

  Scenario: [param-test-011] conflict name
    When executing query:
      """
      $p1=GO FROM "Tim Duncan" OVER like WHERE like.likeness>$p1 yield like._dst as dst;
      GO FROM $p1.dst OVER like YIELD DISTINCT $$.player.name AS name
      """
    Then a SyntaxError should be raised at runtime: Variable definition conflicts with a parameter near `$p1'

  Scenario: [param-test-012] expression with parameters
    When executing query:
      """
      go from "Tim Duncan" over like yield like._dst as id
      | yield $-.id+$p1+hash(abs($p1)+$-.id) as v
      """
    Then the result should be, in any order:
      | v                                    |
      | "Manu Ginobili1-8422829895182987733" |
      | "Tony Parker1803925327675532371"     |
    When executing query:
      """
      $var=go from "Tim Duncan" over like yield like._dst as id;
      yield $var.id+$p1+hash(abs($p1)+$var.id) as v
      """
    Then the result should be, in any order:
      | v                                    |
      | "Manu Ginobili1-8422829895182987733" |
      | "Tony Parker1803925327675532371"     |
    # aggregate expressions
    When executing query:
      """
      $var=go from "Tim Duncan" over like yield like._dst as id, like.likeness as likeness;
      yield avg($var.likeness)+$p1 as v;
      """
    Then the result should be, in any order:
      | v    |
      | 96.0 |
    When executing query:
      """
      go from "Tim Duncan" over like yield like._dst as id
      | yield count(distinct abs($p1)+$-.id) as v
      """
    Then the result should be, in any order:
      | v |
      | 2 |
    # expression nesting
    When executing query:
      """
      go from "Tim Duncan" over like yield properties($$).age as age
      | yield avg(abs(hash($-.age+$p1)+$p1)) as v
      """
    Then the result should be, in any order:
      | v    |
      | 40.5 |
    # BAD_TYPE
    When executing query:
      """
      go from "Tim Duncan" over like yield properties($$).age as age
      | yield abs($-.age+$p3) as v
      """
    Then the result should be, in any order:
      | v        |
      | BAD_TYPE |
      | BAD_TYPE |
    When executing query:
      """
      $var=lookup on player where player.name==$p6.c and player.age in [43,35,42,45] yield id(vertex) AS VertexID;DELETE VERTEX $var.VertexID;RETURN count($var.VertexID) AS record
      """
    Then the execution should be successful
    When executing query:
      """
      $var=lookup on player where player.name==$p3 and player.age in [43,35,42,45] yield id(vertex) AS VertexID;DELETE VERTEX $var.VertexID;RETURN count($var.VertexID) AS record
      """
    Then the execution should be successful
    When executing query:
      """
      $var=lookup on player where player.name==$p7.a.b.d[4] and player.age in [43,35,42,45] yield id(vertex) AS VertexID;DELETE VERTEX $var.VertexID;RETURN count($var.VertexID) AS record
      """
    Then the execution should be successful

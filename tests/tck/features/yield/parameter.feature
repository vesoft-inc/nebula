# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License
Feature: Parameter

  Background:
    Given a graph with space named "nba"
    Given parameters: {"p1":1,"p2":true,"p3":"Tim Duncan","p4":3.3,"p5":[1,true,3],"p6":{"a":3,"b":false,"c":"Tim Duncan"},"p7":{"a":{"b":{"c":"Tim Duncan","d":[1,2,3,true,"Tim Duncan"]}}},"p8":"Manu Ginobili"}

  Scenario: return parameters
    When executing query:
      """
      RETURN abs($p1)+1 AS ival, $p2 and false AS bval, $p3+"ef" AS sval, round($p4)+1.1 AS fval, $p5 AS lval, $p6.a AS mval, all(item in $p7.a.b.d where item<4 or ((item>0) is null)) AS pval
      """
    Then the result should be, in any order:
      | ival | bval  | sval           | fval | lval       | mval | pval |
      | 2    | false | "Tim Duncanef" | 4.1  | [1,true,3] | 3    | true |

  Scenario: cypher with parameters
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
    When executing query:
      """
      MATCH (v:player)
      WITH v AS v WHERE v.player.name in [$p1,$p2,$p3,"Tony Parker",$p4,$p5,$p6]
      RETURN v.player.name AS v ORDER BY v, $p3 LIMIT $p1
      """
    Then the result should be, in order:
      | v            |
      | "Tim Duncan" |

  Scenario: ngql with parameters
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
    When executing query:
      """
      $p1=GO FROM "Tim Duncan" OVER like WHERE like.likeness>$p1 yield like._dst as dst;
      GO FROM $p1.dst OVER like YIELD DISTINCT $$.player.name AS name
      """
    Then a SyntaxError should be raised at runtime: Variable definition conflicts with a parameter near `$p1'
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
      FETCH PROP ON player $nonexist
      """
    Then a SyntaxError should be raised at runtime: Variable is not supported in vid near `$nonexist'
    When executing query:
      """
      FETCH PROP ON player $p3,$p4
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'
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
    When executing query:
      """
      GET SUBGRAPH FROM $p3 BOTH like
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'
    When executing query:
      """
      find shortest path from $p3 to $p2 over like
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'
    When executing query:
      """
      find shortest path from $p3 to $p2 over like
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in vid near `$p3'
    When executing query:
      """
      GO 1 TO 2 STEPS FROM "Tim Duncan" OVER like YIELD like._dst AS dst SAMPLE [1,$p1]
      """
    Then a SyntaxError should be raised at runtime: Parameter is not supported in sample clause near `[1,$p1]'

  Scenario: error check
    When executing query:
      """
      LOOKUP ON player WHERE player.age>$p2+43
      """
    Then a SemanticError should be raised at runtime: Column type error : age
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

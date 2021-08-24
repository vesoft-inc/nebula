# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Parameter

  Background:
    Given a graph with space named "nba"
    Given parameters: {"p1":1,"p2":true,"p3":"Tim Duncan","p4":3.3,"p5":[1,true,3],"p6":{"a":3,"b":false,"c":"Tim Duncan"},"p7":{"a":{"b":{"c":"Tim Duncan"}}}}

  Scenario: return parameters
    When executing query:
      """
      RETURN abs($p1)+1 AS ival, $p2 and false AS bval, $p3+"ef" AS sval, round($p4)+1.1 AS fval, $p5 AS lval, $p6.a AS mval
      """
    Then the result should be, in any order:
      | ival | bval  | sval           | fval | lval       | mval |
      | 2    | false | "Tim Duncanef" | 4.1  | [1,true,3] | 3    |

  Scenario: go with parameters
    When executing query:
      """
      $var=GO FROM $p3 OVER like YIELD like._dst AS dst, $p3+$p1+$p4 AS params, $$.player.age AS age;
      GO FROM $var.dst OVER like YIELD DISTINCT $var.dst, $var.params, $var.age
      """
    Then the result should be, in any order:
      | $var.dst        | $var.params      | $var.age |
      | "Manu Ginobili" | "Tim Duncan13.3" | 41       |
      | "Tony Parker"   | "Tim Duncan13.3" | 36       |
    When executing query:
      """
      GO FROM $p6.c OVER like
      """
    Then the result should be, in any order:
      | like._dst       |
      | "Manu Ginobili" |
      | "Tony Parker"   |

  Scenario: cypher with parameters
    When executing query:
      """
      MATCH (v:player)-[:like]->(n) WHERE id(v)==$p3 and n.age>$p1+29
      RETURN n.name AS dst LIMIT $p1+1
      """
    Then the result should be, in any order:
      | dst             |
      | "Manu Ginobili" |
      | "Tony Parker"   |
    When executing query:
      """
      MATCH (v:player)-[:like]->(n{name:$p7.a.b.c})
      RETURN n.name AS dst LIMIT $p1
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
      | sval           | fval | ival | bval  |
      | "Tim Duncanef" | 4.1  | 2    | false |
    When executing query:
      """
      MATCH (v:player)
      WITH v AS v WHERE v.name in [$p1,$p2,$p3,"Tony Parker",$p4,$p5,$p6]
      RETURN v.name AS v ORDER BY v, $p3 LIMIT $p1
      """
    Then the result should be, in order:
      | v            |
      | "Tim Duncan" |

  Scenario: fetch with parameters
    When executing query:
      """
      FETCH PROP ON * $p3
      """
    Then the result should be, in any order:
      | vertices_                                                                                                   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

  Scenario: subgraph with parameters
    When executing query:
      """
      GET SUBGRAPH FROM $p3 BOTH like
      """
    Then the result should be, in any order:
      | _vertices                              | _edges                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
      | [("Tim Duncan" :bachelor{} :player{})] | [[:like "Aron Baynes"->"Tim Duncan" @0 {}], [:like "Boris Diaw"->"Tim Duncan" @0 {}], [:like "Danny Green"->"Tim Duncan" @0 {}], [:like "Dejounte Murray"->"Tim Duncan" @0 {}], [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {}], [:like "Manu Ginobili"->"Tim Duncan" @0 {}], [:like "Marco Belinelli"->"Tim Duncan" @0 {}], [:like "Shaquile O'Neal"->"Tim Duncan" @0 {}], [:like "Tiago Splitter"->"Tim Duncan" @0 {}], [:like "Tony Parker"->"Tim Duncan" @0 {}], [:like "Tim Duncan"->"Manu Ginobili" @0 {}], [:like "Tim Duncan"->"Tony Parker" @0 {}]] |
      | [("Tim Duncan" :bachelor{} :player{})] | []                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

  Scenario: find path with parameters
    When executing query:
      """
      FIND ALL PATH FROM $p3 TO "Tony Parker" over like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                          |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                                                                                                                |
      | <("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")>                                                                   |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")>                                                              |
      | <("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")> |
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO $p3 over like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                          |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")>                                                                                                   |
      | <("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>                                                                                                 |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>                                                                   |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")>                                                               |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")>                                 |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>                                      |
      | <("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")>                                      |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")>  |
      | <("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tim Duncan")>  |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("LaMarcus Aldridge")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM $p3 TO "Tony Parker" over like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                           |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO $p3 over like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                          |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")-[:like@0 {}]->("Tim Duncan")>   |
      | <("Tim Duncan")-[:like@0 {}]->("Manu Ginobili")-[:like@0 {}]->("Tim Duncan")> |
    When executing query:
      """
      FIND NOLOOP PATH FROM $p3 TO "Tony Parker" over like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                           |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")> |
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO $p3 over like
      """
    Then the result should be, in any order, with relax comparison:
      | path |

  Scenario: error check
    When executing query:
      """
      $p1=GO FROM "Tim Duncan" OVER like WHERE like.likeness>$p1 yield like._dst as dst;
      GO FROM $p1.dst OVER like YIELD DISTINCT $$.player.name AS name
      """
    Then a SyntaxError should be raised at runtime: Variable definition conflicts with a parameter near `$p1'
    When executing query:
      """
      FETCH PROP ON player $nonexist
      """
    Then a SyntaxError should be raised at runtime: Invalid parameter  near `$nonexist'
    When executing query:
      """
      $var=GO FROM $p4 OVER like WHERE like.likeness>$p1 yield like._dst as dst;
      GO FROM $p1.dst OVER like YIELD DISTINCT $$.player.name AS name
      """
    Then a SemanticError should be raised at runtime: `$p4', the srcs should be type of FIXED_STRING, but was`FLOAT'
    When executing query:
      """
      GO FROM $p3 OVER like WHERE like.likeness>toFloat($p6) yield like._dst as dst;
      """
    Then a SemanticError should be raised at runtime: `toFloat($p6)' is not a valid expression : Parameter's type error
    When executing query:
      """
      MATCH (v:player) RETURN  v LIMIT $p6
      """
    Then a SemanticError should be raised at runtime: LIMIT should be of type integer
    When executing query:
      """
      GO FROM $p3,$p4 OVER like
      """
    Then a SyntaxError should be raised at runtime: syntax error near `,$p4 OVE'
    When executing query:
      """
      FETCH PROP ON player $p3,$p4
      """
    Then a SyntaxError should be raised at runtime:syntax error near `,$p4'
    When executing query:
      """
      find noloop path from $p3 to $p2 over like
      """
    Then a SemanticError should be raised at runtime: `$p2', the srcs should be type of FIXED_STRING, but was`BOOL'
    When executing query:
      """
      find all path from $p3 to $p2 over like
      """
    Then a SemanticError should be raised at runtime: `$p2', the srcs should be type of FIXED_STRING, but was`BOOL'
    When executing query:
      """
      find shortest path from $p3 to $p2 over like
      """
    Then a SemanticError should be raised at runtime: `$p2', the srcs should be type of FIXED_STRING, but was`BOOL'

  @skip
  Scenario: lookup with parameters
    When executing query:
      """
      # parameter push-down not supported yet
      LOOKUP ON player WHERE player.age>$p2+43
      """
    Then the result should be, in any order:
      | VertexID          |
      | "Grant Hill"      |
      | "Jason Kidd"      |
      | "Shaquile O'Neal" |
      | "Steve Nash"      |

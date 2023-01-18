# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Multi Query Parts

  Background:
    Given a graph with space named "nba"

  Scenario: Multi Path Patterns
    When executing query:
      """
      MATCH (m)-[]-(n), (n)-[]-(l) WHERE id(m)=="Tim Duncan"
      RETURN m.player.name AS n1, n.player.name AS n2,
      CASE WHEN l.team.name is not null THEN l.team.name
      WHEN l.player.name is not null THEN l.player.name ELSE "null" END AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3           |
      | "Tim Duncan" | "Aron Baynes" | "Celtics"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons"    |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"      |
      | "Tim Duncan" | "Aron Baynes" | "Tim Duncan" |
      | "Tim Duncan" | "Boris Diaw"  | "Hawks"      |
      | "Tim Duncan" | "Boris Diaw"  | "Hornets"    |
      | "Tim Duncan" | "Boris Diaw"  | "Jazz"       |
      | "Tim Duncan" | "Boris Diaw"  | "Spurs"      |
      | "Tim Duncan" | "Boris Diaw"  | "Suns"       |
      | "Tim Duncan" | "Boris Diaw"  | "Tim Duncan" |
    When executing query:
      """
      MATCH (m)-[]-(n), (n)-[]-(l) WHERE id(n)=="Tim Duncan"
      RETURN m.player.name AS n1, n.player.name AS n2, l.player.name AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then the result should be, in order:
      | n1            | n2           | n3                  |
      | "Aron Baynes" | "Tim Duncan" | "Aron Baynes"       |
      | "Aron Baynes" | "Tim Duncan" | "Boris Diaw"        |
      | "Aron Baynes" | "Tim Duncan" | "Danny Green"       |
      | "Aron Baynes" | "Tim Duncan" | "Danny Green"       |
      | "Aron Baynes" | "Tim Duncan" | "Dejounte Murray"   |
      | "Aron Baynes" | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Aron Baynes" | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Aron Baynes" | "Tim Duncan" | "Manu Ginobili"     |
      | "Aron Baynes" | "Tim Duncan" | "Manu Ginobili"     |
      | "Aron Baynes" | "Tim Duncan" | "Manu Ginobili"     |
    When executing query:
      """
      MATCH (m)-[]-(n), (n)-[]-(l), (l)-[]-(h) WHERE id(m)=="Tim Duncan"
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                 |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"        |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquille O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"       |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"       |
    # Below scenario is not supported for the execution plan has a scan.
    When executing query:
      """
      MATCH (m)-[]-(n), (a)-[]-(c) WHERE id(m)=="Tim Duncan"
      RETURN m,n,a,c
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: Multi Match
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      MATCH (n)-[]-(l)
      RETURN m.player.name AS n1, n.player.name AS n2,
      CASE WHEN l.player.name is not null THEN l.player.name
      WHEN l.team.name is not null THEN l.team.name ELSE "null" END AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3           |
      | "Tim Duncan" | "Aron Baynes" | "Celtics"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons"    |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"      |
      | "Tim Duncan" | "Aron Baynes" | "Tim Duncan" |
      | "Tim Duncan" | "Boris Diaw"  | "Hawks"      |
      | "Tim Duncan" | "Boris Diaw"  | "Hornets"    |
      | "Tim Duncan" | "Boris Diaw"  | "Jazz"       |
      | "Tim Duncan" | "Boris Diaw"  | "Spurs"      |
      | "Tim Duncan" | "Boris Diaw"  | "Suns"       |
      | "Tim Duncan" | "Boris Diaw"  | "Tim Duncan" |
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      MATCH (n)-[]-(l), (l)-[]-(h)
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                 |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"        |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquille O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"       |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"       |
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      MATCH (n)-[]-(l)
      MATCH (l)-[]-(h)
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                 |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"        |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquille O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"       |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"       |
    When executing query:
      """
      MATCH (v:player{name:"Tony Parker"})
      WITH v AS a
      MATCH p=(o:player{name:"Tim Duncan"})-[]->(a)
      RETURN o.player.name
      """
    Then the result should be, in order:
      | o.player.name |
      | "Tim Duncan"  |
      | "Tim Duncan"  |
    # Both the 2 match statement contains variable v1 and v4
    When profiling query:
      """
      MATCH (v1:player)-[:like*2..2]->(v2)-[e3:like]->(v4) where id(v1) == "Tony Parker"
      MATCH (v3:player)-[:like]->(v1)<-[e5]-(v4) where id(v3) == "Tim Duncan" return *
      """
    Then the result should be, in any order, with relax comparison:
      | v1              | v2                | e3                                                           | v4                    | v3             | e5                                            |
      | ("Tony Parker") | ("Tony Parker")   | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        | ("Tim Duncan")        | ("Tim Duncan") | [:teammate "Tim Duncan"->"Tony Parker" @0]    |
      | ("Tony Parker") | ("Manu Ginobili") | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      | ("Tim Duncan")        | ("Tim Duncan") | [:teammate "Tim Duncan"->"Tony Parker" @0]    |
      | ("Tony Parker") | ("Tony Parker")   | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     | ("Manu Ginobili")     | ("Tim Duncan") | [:teammate "Manu Ginobili"->"Tony Parker" @0] |
      | ("Tony Parker") | ("Tony Parker")   | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     | ("Manu Ginobili")     | ("Tim Duncan") | [:teammate "Manu Ginobili"->"Tony Parker" @0] |
      | ("Tony Parker") | ("Tim Duncan" )   | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      | ("Manu Ginobili")     | ("Tim Duncan") | [:teammate "Manu Ginobili"->"Tony Parker" @0] |
      | ("Tony Parker") | ("Tim Duncan" )   | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      | ("Manu Ginobili")     | ("Tim Duncan") | [:teammate "Manu Ginobili"->"Tony Parker" @0] |
      | ("Tony Parker") | ("Tony Parker")   | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] | ("LaMarcus Aldridge") | ("Tim Duncan") | [:like "LaMarcus Aldridge"->"Tony Parker" @0] |
    # The redudant Project after HashInnerJoin is removed now
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 19 | HashInnerJoin  | 7,14         |                |               |
      | 7  | Project        | 6            |                |               |
      | 6  | AppendVertices | 5            |                |               |
      | 5  | Traverse       | 20           |                |               |
      | 20 | Traverse       | 2            |                |               |
      | 2  | Dedup          | 1            |                |               |
      | 1  | PassThrough    | 3            |                |               |
      | 3  | Start          |              |                |               |
      | 14 | Project        | 12           |                |               |
      | 12 | Traverse       | 21           |                |               |
      | 21 | Traverse       | 9            |                |               |
      | 9  | Dedup          | 8            |                |               |
      | 8  | PassThrough    | 10           |                |               |
      | 10 | Start          |              |                |               |

  Scenario: Optional Match
    When profiling query:
      """
      MATCH (v1:player)-[:like*2..2]->(v2)-[e3:like]->(v4) where id(v1) == "Tony Parker"
      OPTIONAL MATCH (v3:player)-[:like]->(v1)<-[e5]-(v4)
      with v1, v2, e3, v4, e5, v3 where id(v3) == "Tim Duncan" or id(v3) is NULL
      return *
      """
    Then the result should be, in any order, with relax comparison:
      | v1              | v2                | e3                                            | v4                    | e5                                             | v3             |
      | ("Tony Parker") | ("Tony Parker")   | [:like "Tony Parker"->"LaMarcus Aldridge" @0] | ("LaMarcus Aldridge") | [:like "LaMarcus Aldridge"->"Tony Parker" @0]  | ("Tim Duncan") |
      | ("Tony Parker") | ("Tim Duncan")    | [:like "Tim Duncan"->"Tony Parker" @0 ]       | ("Tony Parker")       | NULL                                           | NULL           |
      | ("Tony Parker") | ("Tim Duncan")    | [:like "Tim Duncan"->"Tony Parker" @0]        | ("Tony Parker")       | NULL                                           | NULL           |
      | ("Tony Parker") | ("Tony Parker")   | [:like "Tony Parker"->"Tim Duncan" @0 ]       | ("Tim Duncan")        | [:teammate "Tim Duncan"->"Tony Parker" @0]     | ("Tim Duncan") |
      | ("Tony Parker") | ("Manu Ginobili") | [:like "Manu Ginobili"->"Tim Duncan" @0 ]     | ("Tim Duncan")        | [:teammate "Tim Duncan"->"Tony Parker" @0]     | ("Tim Duncan") |
      | ("Tony Parker") | ("Tony Parker")   | [:like "Tony Parker"->"Manu Ginobili" @0]     | ("Manu Ginobili")     | [:teammate "Manu Ginobili"->"Tony Parker" @0]  | ("Tim Duncan") |
      | ("Tony Parker") | ("Tony Parker")   | [:like "Tony Parker"->"Manu Ginobili" @0 ]    | ("Manu Ginobili")     | [:teammate "Manu Ginobili"->"Tony Parker" @0]  | ("Tim Duncan") |
      | ("Tony Parker") | ("Tim Duncan")    | [:like "Tim Duncan"->"Manu Ginobili" @0 ]     | ("Manu Ginobili")     | [:teammate "Manu Ginobili"->"Tony Parker" @0 ] | ("Tim Duncan") |
      | ("Tony Parker") | ("Tim Duncan")    | [:like "Tim Duncan"->"Manu Ginobili" @0]      | ("Manu Ginobili")     | [:teammate "Manu Ginobili"->"Tony Parker" @0]  | ("Tim Duncan") |
    # The redudant Project after HashLeftJoin is removed now
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 22 | Project        | 18           |                |               |
      | 18 | Filter         | 14           |                |               |
      | 14 | HashLeftJoin   | 7,13         |                |               |
      | 7  | project        | 6            |                |               |
      | 6  | AppendVertices | 5            |                |               |
      | 5  | Traverse       | 20           |                |               |
      | 20 | Traverse       | 2            |                |               |
      | 2  | Dedup          | 1            |                |               |
      | 1  | PassThrough    | 3            |                |               |
      | 3  | Start          |              |                |               |
      | 13 | Project        | 21           |                |               |
      | 21 | AppendVertices | 11           |                |               |
      | 11 | Traverse       | 10           |                |               |
      | 10 | AppendVertices | 9            |                |               |
      | 9  | Traverse       | 8            |                |               |
      | 8  | Argument       |              |                |               |
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      OPTIONAL MATCH (n)<-[:serve]-(l)
      RETURN m.player.name AS n1, n.player.name AS n2, l AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2                  | n3   |
      | "Tim Duncan" | "Aron Baynes"       | NULL |
      | "Tim Duncan" | "Boris Diaw"        | NULL |
      | "Tim Duncan" | "Danny Green"       | NULL |
      | "Tim Duncan" | "Danny Green"       | NULL |
      | "Tim Duncan" | "Dejounte Murray"   | NULL |
      | "Tim Duncan" | "LaMarcus Aldridge" | NULL |
      | "Tim Duncan" | "LaMarcus Aldridge" | NULL |
      | "Tim Duncan" | "Manu Ginobili"     | NULL |
      | "Tim Duncan" | "Manu Ginobili"     | NULL |
      | "Tim Duncan" | "Manu Ginobili"     | NULL |
    # Below scenario is not supported for the execution plan has a scan.
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      OPTIONAL MATCH (a)<-[]-(b)
      RETURN m.player.name AS n1, n.player.name AS n2, a.player.name AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: Multi Query Parts
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      WITH n, n.player.name AS n1 ORDER BY n1 LIMIT 10
      MATCH (n)-[]-(l)
      RETURN n.player.name AS n1,
      CASE WHEN l.player.name is not null THEN l.player.name
      WHEN l.team.name is not null THEN l.team.name ELSE "null" END AS n2 ORDER BY n1, n2 LIMIT 10
      """
    Then the result should be, in order:
      | n1            | n2           |
      | "Aron Baynes" | "Celtics"    |
      | "Aron Baynes" | "Pistons"    |
      | "Aron Baynes" | "Spurs"      |
      | "Aron Baynes" | "Tim Duncan" |
      | "Boris Diaw"  | "Hawks"      |
      | "Boris Diaw"  | "Hornets"    |
      | "Boris Diaw"  | "Jazz"       |
      | "Boris Diaw"  | "Spurs"      |
      | "Boris Diaw"  | "Suns"       |
      | "Boris Diaw"  | "Tim Duncan" |
    When executing query:
      """
      MATCH (m:player{name:"Tim Duncan"})-[:like]-(n)--()
      WITH  m,count(*) AS lcount
      MATCH (m)--(n)
      RETURN count(*) AS scount, lcount
      """
    Then the result should be, in order:
      | scount | lcount |
      | 19     | 110    |
    When executing query:
      """
      MATCH (m:player{name:"Tim Duncan"})-[:like]-(n)--()
      WITH  m,n
      MATCH (m)--(n)
      RETURN count(*) AS scount
      """
    Then the result should be, in order:
      | scount |
      | 270    |
    # Below scenario is not supported for the execution plan has a scan.
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      WITH n, n.player.name AS n1 ORDER BY n1 LIMIT 10
      MATCH (a)-[]-(b)
      RETURN a.player.name AS n1, b.player.name AS n2 ORDER BY n1, n2 LIMIT 10
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: Some Errors
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      WITH n, n.player.name AS n1 ORDER BY n1 LIMIT 10
      RETURN m
      """
    Then a SemanticError should be raised at runtime: Alias used but not defined: `m'

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
@jie
Feature: Multi Query Parts

  Background:
    Given a graph with space named "nba"

  Scenario: Multi Path Patterns
    When profiling query:
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
    # And the execution plan should be:
    # | id | name           | dependencies | operator info                                                                            |
    # | 15 | DataCollect    | 16           |                                                                                          |
    # | 16 | TopN           | 12           |                                                                                          |
    # | 12 | Project        | 18           |                                                                                          |
    # | 18 | Project        | 17           |                                                                                          |
    # | 17 | Filter         | 9            |                                                                                          |
    # | 9  | BiInnerJoin    | 5, 8         |                                                                                          |
    # | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":2}]" }                                     |
    # | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":2}]" }                               |
    # | 2  | Dedup          | 1            |                                                                                          |
    # | 1  | PassThrough    | 3            |                                                                                          |
    # | 3  | Start          |              |                                                                                          |
    # | 8  | AppendVertices | 7            | {  "props": "[{\"tagId\":2,\"props\":[\"name\"]}, {\"tagId\":3,\"props\":[\"name\"]}]" } |
    # | 7  | Traverse       | 6            | { "vertexProps": "[{\"tagId\":2,\"props\":[\"name\"]}]" }                                |
    # | 6  | Argument       |              |                                                                                          |
    When profiling query:
      """
      MATCH (m)-[]-(n), (l)-[]-(n) WHERE id(m)=="Tim Duncan"
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
    When profiling query:
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
    When profiling query:
      """
      MATCH (m)-[]-(n), (n)-[]-(l), (l)-[]-(h) WHERE id(m)=="Tim Duncan"
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"    |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"       |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquile O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"   |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"      |
    # And the execution plan should be:
    # | id | name           | dependencies | operator info                                                                            |
    # | 19 | DataCollect    | 20           |                                                                                          |
    # | 20 | TopN           | 23           |                                                                                          |
    # | 23 | Project        | 21           |                                                                                          |
    # | 21 | Filter         | 13           |                                                                                          |
    # | 13 | BiInnerJoin    | 9, 12        |                                                                                          |
    # | 9  | BiInnerJoin    | 5, 8         |                                                                                          |
    # | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":2}]" }                                     |
    # | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":2}]" }                               |
    # | 2  | Dedup          | 1            |                                                                                          |
    # | 1  | PassThrough    | 3            |                                                                                          |
    # | 3  | Start          |              |                                                                                          |
    # | 8  | AppendVertices | 7            | {  "props": "[{\"tagId\":2,\"props\":[\"name\"]}, {\"tagId\":3,\"props\":[\"name\"]}]" } |
    # | 7  | Traverse       | 6            | { "vertexProps": "[{\"tagId\":2,\"props\":[\"name\"]}]" }                                |
    # | 6  | Argument       |              |                                                                                          |
    # | 12 | AppendVertices | 11           | {  "props": "[{\"props\":[\"name\"],\"tagId\":2}]" }                                     |
    # | 11 | Traverse       | 10           | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":3}]" }                               |
    # | 10 | Argument       |              |                                                                                          |
    # Below scenario is not suppoted for the execution plan has a scan.
    When executing query:
      """
      MATCH (m)-[]-(n), (a)-[]-(c) WHERE id(m)=="Tim Duncan"
      RETURN m,n,a,c
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: Multi Match
    When profiling query:
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
    # And the execution plan should be:
    # | id | name           | dependencies | operator info                                                                            |
    # | 16 | DataCollect    | 17           |                                                                                          |
    # | 17 | TopN           | 13           |                                                                                          |
    # | 13 | Project        | 12           |                                                                                          |
    # | 12 | BiInnerJoin    | 19, 11       |                                                                                          |
    # | 19 | Project        | 18           |                                                                                          |
    # | 18 | Filter         | 5            |                                                                                          |
    # | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":2}]" }                                     |
    # | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":2}]" }                               |
    # | 2  | Dedup          | 1            |                                                                                          |
    # | 1  | PassThrough    | 3            |                                                                                          |
    # | 3  | Start          |              |                                                                                          |
    # | 11 | Project        | 10           |                                                                                          |
    # | 10 | AppendVertices | 9            | {  "props": "[{\"tagId\":2,\"props\":[\"name\"]}, {\"tagId\":3,\"props\":[\"name\"]}]" } |
    # | 9  | Traverse       | 8            | { "vertexProps": "[{\"tagId\":2,\"props\":[\"name\"]}]" }                                |
    # | 8  | Argument       |              |                                                                                          |
    When profiling query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      MATCH (n)-[]-(l), (l)-[]-(h)
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"    |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"       |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquile O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"   |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"      |
    # And the execution plan should be:
    # | id | name           | dependencies | operator info                                                                            |
    # | 20 | DataCollect    | 21           |                                                                                          |
    # | 21 | TopN           | 17           |                                                                                          |
    # | 17 | Project        | 16           |                                                                                          |
    # | 16 | BiInnerJoin    | 23, 15       |                                                                                          |
    # | 23 | Project        | 22           |                                                                                          |
    # | 22 | Filter         | 5            |                                                                                          |
    # | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":2}]" }                                     |
    # | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":2}]" }                               |
    # | 2  | Dedup          | 1            |                                                                                          |
    # | 1  | PassThrough    | 3            |                                                                                          |
    # | 3  | Start          |              |                                                                                          |
    # | 15 | Project        | 14           |                                                                                          |
    # | 14 | BiInnerJoin    | 10, 13       |                                                                                          |
    # | 10 | AppendVertices | 9            | {  "props": "[{\"tagId\":2,\"props\":[\"name\"]}, {\"tagId\":3,\"props\":[\"name\"]}]" } |
    # | 9  | Traverse       | 8            | { "vertexProps": "[{\"tagId\":2,\"props\":[\"name\"]}]" }                                |
    # | 8  | Argument       |              |                                                                                          |
    # | 13 | AppendVertices | 12           | {  "props": "[{\"tagId\":2,\"props\":[\"name\"]}, {\"tagId\":3,\"props\":[\"name\"]}]" } |
    # | 12 | Traverse       | 11           | { "vertexProps": "[{\"tagId\":3,\"props\":[\"name\"]}]" }                                |
    # | 11 | Argument       |              |                                                                                          |
    When profiling query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      MATCH (n)-[]-(l)
      MATCH (l)-[]-(h)
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"    |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"       |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquile O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"   |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"     |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"      |
    When profiling query:
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

  # And the execution plan should be:
  # | id | name           | dependencies | operator info                                                                                                                                                                                                                                   |
  # | 10 | Project        | 11           |                                                                                                                                                                                                                                                 |
  # | 11 | BiInnerJoin    | 14, 9        |                                                                                                                                                                                                                                                 |
  # | 14 | Project        | 3            |                                                                                                                                                                                                                                                 |
  # | 3  | AppendVertices | 12           | {  "props": "[{\"props\":[\"name\"],\"tagId\":2}]" }                                                                                                                                                                                            |
  # | 12 | IndexScan      | 2            |                                                                                                                                                                                                                                                 |
  # | 2  | Start          |              |                                                                                                                                                                                                                                                 |
  # | 9  | Project        | 8            |                                                                                                                                                                                                                                                 |
  # | 8  | AppendVertices | 7            | {  "props": "[{\"props\":[\"name\", \"_tag\"],\"tagId\":3}, {\"props\":[\"name\", \"age\", \"_tag\"],\"tagId\":2}, {\"props\":[\"name\", , \"speciality\", \"_tag\"],\"tagId\":4}, {\"props\":[\"id\", \"ts\", \"_tag\"],\"tagId\":6}]" }       |
  # | 7  | Traverse       | 6            | {  "vertexProps": "[{\"props\":[\"name\", \"_tag\"],\"tagId\":3}, {\"props\":[\"name\", \"age\", \"_tag\"],\"tagId\":2}, {\"props\":[\"name\", , \"speciality\", \"_tag\"],\"tagId\":4}, {\"props\":[\"id\", \"ts\", \"_tag\"],\"tagId\":6}]" } |
  # | 6  | Argument       |              |                                                                                                                                                                                                                                                 |
  Scenario: Optional Match
    When profiling query:
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
    # And the execution plan should be:
    # | id | name           | dependencies | operator info                                                                                                                                                                                                                             |
    # | 16 | DataCollect    | 17           |                                                                                                                                                                                                                                           |
    # | 17 | TopN           | 13           |                                                                                                                                                                                                                                           |
    # | 13 | Project        | 12           |                                                                                                                                                                                                                                           |
    # | 12 | BiLeftJoin     | 19, 11       |                                                                                                                                                                                                                                           |
    # | 19 | Project        | 18           |                                                                                                                                                                                                                                           |
    # | 18 | Filter         | 5            |                                                                                                                                                                                                                                           |
    # | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":2}]" }                                                                                                                                                                                      |
    # | 4  | Traverse       | 2            |                                                                                                                                                                                                                                           |
    # | 2  | Dedup          | 1            |                                                                                                                                                                                                                                           |
    # | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                           |
    # | 3  | Start          |              |                                                                                                                                                                                                                                           |
    # | 11 | Project        | 10           |                                                                                                                                                                                                                                           |
    # | 10 | AppendVertices | 9            | {  "props": "[{\"props\":[\"name\", \"_tag\"],\"tagId\":3}, {\"props\":[\"name\", \"age\", \"_tag\"],\"tagId\":2}, {\"props\":[\"name\", , \"speciality\", \"_tag\"],\"tagId\":4}, {\"props\":[\"id\", \"ts\", \"_tag\"],\"tagId\":6}]" } |
    # | 9  | Traverse       | 8            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":2}]" }                                                                                                                                                                                |
    # | 8  | Argument       |              |                                                                                                                                                                                                                                           |
    # Below scenario is not suppoted for the execution plan has a scan.
    When profiling query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      OPTIONAL MATCH (a)<-[]-(b)
      RETURN m.player.name AS n1, n.player.name AS n2, a.player.name AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: Multi Query Parts
    When profiling query:
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
    When profiling query:
      """
      MATCH (m:player{name:"Tim Duncan"})-[:like]-(n)--()
      WITH  m,count(*) AS lcount
      MATCH (m)--(n)
      RETURN count(*) AS scount, lcount
      """
    Then the result should be, in order:
      | scount | lcount |
      | 19     | 110    |
    When profiling query:
      """
      MATCH (m:player{name:"Tim Duncan"})-[:like]-(n)--()
      WITH  m,n
      MATCH (m)--(n)
      RETURN count(*) AS scount
      """
    Then the result should be, in order:
      | scount |
      | 270    |
    # And the execution plan should be:
    # | id | name           | dependencies | operator info                                              |
    # | 12 | Aggregate      | 13           |                                                            |
    # | 13 | BiInnerJoin    | 15, 11       |                                                            |
    # | 15 | Project        | 5            |                                                            |
    # | 5  | AppendVertices | 4            | {  "props": "[]" }                                         |
    # | 4  | Traverse       | 3            | { "vertexProps": "" }                                      |
    # | 3  | Traverse       | 14           | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":2}]" } |
    # | 14 | IndexScan      | 2            |                                                            |
    # | 2  | Start          |              |                                                            |
    # | 11 | Project        | 10           |                                                            |
    # | 10 | AppendVertices | 9            | {  "props": "[]" }                                         |
    # | 9  | Traverse       | 8            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":2}]" } |
    # | 8  | Argument       |              |                                                            |
    # Below scenario is not suppoted for the execution plan has a scan.
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      WITH n, n.player.name AS n1 ORDER BY n1 LIMIT 10
      MATCH (a)-[]-(b)
      RETURN a.player.name AS n1, b.player.name AS n2 ORDER BY n1, n2 LIMIT 10
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: Some Erros
    When profiling query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      WITH n, n.player.name AS n1 ORDER BY n1 LIMIT 10
      RETURN m
      """
    Then a SemanticError should be raised at runtime: Alias used but not defined: `m'
    When profiling query:
      """
      MATCH (v:player)-[e]-(v:team) RETURN v, e
      """
    Then a SemanticError should be raised at runtime: `v': Redefined alias in a single path pattern.

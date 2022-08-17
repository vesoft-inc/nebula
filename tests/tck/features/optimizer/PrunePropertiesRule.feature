# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Prune Properties rule

  Background:
    Given a graph with space named "nba"

  # The schema id is not fixed in standalone cluster, so we skip it
  @distonly
  Scenario: Single Match
    When profiling query:
      """
      MATCH p = (v:player{name: "Tony Parker"})-[e:like]->(v2)
      RETURN v2.player.age
      """
    # Since in distributed system and scatter gather model, the order is not guarrented.
    Then the result should be, in any order:
      | v2.player.age |
      | 42            |
      | 33            |
      | 41            |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                    |
      | 8  | Project        | 4            |                                                                                                  |
      | 4  | AppendVertices | 3            | {  "props": "[{\"props\":[\"age\"],\"tagId\":9}]" }                                              |
      | 3  | Traverse       | 7            | {  "vertexProps": "", "edgeProps": "[{\"props\":[\"_dst\", \"_rank\", \"_type\"],\"type\":3}]" } |
      | 7  | IndexScan      | 2            |                                                                                                  |
      | 2  | Start          |              |                                                                                                  |
    When profiling query:
      """
      MATCH p = (v:player{name: "Tony Parker"})-[e:like]->(v2)
      RETURN v.player.name
      """
    Then the result should be, in order:
      | v.player.name |
      | "Tony Parker" |
      | "Tony Parker" |
      | "Tony Parker" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                        |
      | 8  | Project        | 4            |                                                                                                                                      |
      | 4  | AppendVertices | 3            | {  "props": "[{\"tagId\": 9, \"props\": [\"_tag\"]}]"}                                                                               |
      | 3  | Traverse       | 7            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":9}]", "edgeProps": "[{\"props\":[\"_dst\", \"_rank\", \"_type\"],\"type\":3}]" } |
      | 7  | IndexScan      | 2            |                                                                                                                                      |
      | 2  | Start          |              |                                                                                                                                      |
    When profiling query:
      """
      MATCH p = (v:player{name: "Tony Parker"})-[e:like]-(v2)
      RETURN v
      """
    Then the result should be, in order:
      | v                                                     |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                                                                                                                |
      | 8  | Project        | 4            |                                                                                                                                                                                                                                                                                                                                              |
      | 4  | AppendVertices | 3            | {  "props": "[{\"tagId\": 9, \"props\": [\"_tag\"]} ]" }                                                                                                                                                                                                                                                                                     |
      | 3  | Traverse       | 7            | { "vertexProps": "[{\"props\": [\"name\", \"age\", \"_tag\"], \"tagId\": 9}, {\"props\": [\"name\", \"speciality\", \"_tag\"], \"tagId\": 8}, {\"tagId\": 10, \"props\": [\"name\", \"_tag\"]}]", "edgeProps": "[{\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": -3}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 3}]" } |
      | 7  | IndexScan      | 2            |                                                                                                                                                                                                                                                                                                                                              |
      | 2  | Start          |              |                                                                                                                                                                                                                                                                                                                                              |
    When profiling query:
      """
      MATCH p = (v:player{name: "Tony Parker"})-[e:like]->(v2)
      RETURN v2
      """
    Then the result should be, in any order:
      | v2                                                                                                          |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                                         |
      | 8  | Project        | 4            |                                                                                                                                                                                       |
      | 4  | AppendVertices | 3            | {  "props": "[{\"props\":[\"name\", \"age\", \"_tag\"],\"tagId\":9}, {\"props\":[\"name\", \"speciality\", \"_tag\"], \"tagId\":8}, {\"props\":[\"name\", \"_tag\"],\"tagId\":10}]" } |
      | 3  | Traverse       | 7            | {  "vertexProps": "", "edgeProps": "[{\"props\":[\"_dst\", \"_rank\", \"_type\"],\"type\":3}]" }                                                                                      |
      | 7  | IndexScan      | 2            |                                                                                                                                                                                       |
      | 2  | Start          |              |                                                                                                                                                                                       |
    # The rule will not take affect in this case because it returns the whole path
    When executing query:
      """
      MATCH p = (v:player{name: "Tony Parker"})-[e:like]-(v2)
      RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                             |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 99}]-("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})>                                               |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                   |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 80}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})>                                                         |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 75}]-("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})>                                           |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})>                                           |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 95}]-("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 50}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})>                                               |
    When profiling query:
      """
      MATCH p = (v:player{name: "Tony Parker"})-[e:like]->(v2)
      RETURN type(e)
      """
    Then the result should be, in order:
      | type(e) |
      | "like"  |
      | "like"  |
      | "like"  |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                    |
      | 8  | Project        | 4            |                                                                                                  |
      | 4  | AppendVertices | 3            | {  "props": "[{\"props\":[\"_tag\"],\"tagId\":9} ]" }                                            |
      | 3  | Traverse       | 7            | {  "vertexProps": "", "edgeProps": "[{\"props\":[\"_dst\", \"_rank\", \"_type\"],\"type\":3}]" } |
      | 7  | IndexScan      | 2            |                                                                                                  |
      | 2  | Start          |              |                                                                                                  |
    When executing query:
      """
      MATCH (v:player{name: "Tony Parker"})-[:like]-(v2)--(v3)
      WITH v3, v3.player.age AS age
      RETURN v3, age ORDER BY age LIMIT 3
      """
    Then the result should be, in order:
      | v3                                                          | age |
      | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})   | 25  |
      | ("Damian Lillard" :player{age: 28, name: "Damian Lillard"}) | 28  |
      | ("Damian Lillard" :player{age: 28, name: "Damian Lillard"}) | 28  |
    When executing query:
      """
      MATCH (v:player{name: "Tony Parker"})-[:serve]->(t:team)
      WITH DISTINCT v, t
      RETURN t
      """
    Then the result should be, in any order:
      | t                                  |
      | ("Spurs" :team{name: "Spurs"})     |
      | ("Hornets" :team{name: "Hornets"}) |
    When executing query:
      """
      MATCH (v:player{name: "Tony Parker"})-[:serve]->(t:team)
      WITH DISTINCT v.player.age as age, t
      RETURN t
      """
    Then the result should be, in any order:
      | t                                  |
      | ("Spurs" :team{name: "Spurs"})     |
      | ("Hornets" :team{name: "Hornets"}) |

  # The schema id is not fixed in standalone cluster, so we skip it
  @distonly
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
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
      | 16 | TopN           | 12           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 12 | Project        | 9            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 9  | BiInnerJoin    | 22, 23       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 22 | Project        | 5            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
      | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 3  | Start          |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 23 | Project        | 8            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 8  | AppendVertices | 7            | {  "props": "[{\"tagId\":9,\"props\":[\"name\"]}, {\"tagId\":10,\"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 7  | Traverse       | 6            | { "vertexProps": "[{\"tagId\":9,\"props\":[\"name\"]}]", "edgeProps": "[{\"type\": -5, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"type\": 5, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": -3}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 3}, {\"type\": -4, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 4}]" } |
      | 6  | Argument       |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
    When profiling query:
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
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
      | 20 | TopN           | 23           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
      | 23 | Project        | 13           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
      | 13 | BiInnerJoin    | 9, 30        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
      | 9  | BiInnerJoin    | 28, 29       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
      | 28 | Project        | 5            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":9}]", "edgeProps": "[{\"type\": -5, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"type\": 5, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": -3}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 3}, {\"type\": -4, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 4}]" }  |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 3  | Start          |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 29 | Project        | 8            |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 8  | AppendVertices | 7            | {  "props": "[{\"tagId\":10,\"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                                                                                                   |
      | 7  | Traverse       | 6            | { "vertexProps": "[{\"tagId\":9,\"props\":[\"name\"]}]", "edgeProps": "[{\"type\": -5, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"type\": 5, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": -3}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 3}, {\"type\": -4, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 4}]" }   |
      | 6  | Argument       |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 31 | Start          |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 30 | Project        | 12           |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 12 | AppendVertices | 11           | {  "props": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 11 | Traverse       | 10           | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":10}]", "edgeProps": "[{\"type\": -5, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"type\": 5, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": -3}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 3}, {\"type\": -4, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 4}]" } |
      | 10 | Argument       |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |

  # The schema id is not fixed in standalone cluster, so we skip it
  @distonly
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
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                             |
      | 17 | TopN           | 13           |                                                                                           |
      | 13 | Project        | 12           |                                                                                           |
      | 12 | BiInnerJoin    | 19, 11       |                                                                                           |
      | 19 | Project        | 5            |                                                                                           |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                      |
      | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                |
      | 2  | Dedup          | 1            |                                                                                           |
      | 1  | PassThrough    | 3            |                                                                                           |
      | 3  | Start          |              |                                                                                           |
      | 11 | Project        | 10           |                                                                                           |
      | 10 | AppendVertices | 9            | {  "props": "[{\"tagId\":9,\"props\":[\"name\"]}, {\"tagId\":10,\"props\":[\"name\"]}]" } |
      | 9  | Traverse       | 8            | { "vertexProps": "[{\"tagId\":9,\"props\":[\"name\"]}]" }                                 |
      | 8  | Argument       |              |                                                                                           |
      | 33 | Start          |              |                                                                                           |
    When profiling query:
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
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
      | 21 | TopN           | 17           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 17 | Project        | 16           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 16 | BiInnerJoin    | 23, 14       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 23 | Project        | 5            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
      | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 3  | Start          |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 14 | BiInnerJoin    | 33, 34       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 33 | Project        | 10           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 10 | AppendVertices | 9            | {  "props": "[{\"tagId\":10,\"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
      | 9  | Traverse       | 8            | { "vertexProps": "[{\"tagId\":9,\"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
      | 8  | Argument       |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 35 | Start          |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 34 | Project        | 13           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | 13 | AppendVertices | 12           | {  "props": "[{\"tagId\":9,\"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
      | 12 | Traverse       | 11           | { "vertexProps": "[{\"tagId\":10,\"props\":[\"name\"]}]", "edgeProps": "[{\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": -5}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 5}, {\"type\": -3, \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 3}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": -4}, {\"props\": [\"_dst\", \"_rank\", \"_type\"], \"type\": 4}]" } |
      | 11 | Argument       |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
      | 36 | Start          |              |                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
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
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                                                                                  |
      | 10 | Project        | 11           |                                                                                                                                                                                                                                |
      | 11 | BiInnerJoin    | 14, 9        |                                                                                                                                                                                                                                |
      | 14 | Project        | 3            |                                                                                                                                                                                                                                |
      | 3  | AppendVertices | 12           | {  "props": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                                                           |
      | 12 | IndexScan      | 2            |                                                                                                                                                                                                                                |
      | 2  | Start          |              |                                                                                                                                                                                                                                |
      | 9  | Project        | 8            |                                                                                                                                                                                                                                |
      | 8  | AppendVertices | 7            | {  "props": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                                                           |
      | 7  | Traverse       | 6            | {  "vertexProps": "", "edgeProps": "[{\"type\": -5, \"props\": [\"_type\", \"_rank\", \"_dst\"]}, {\"props\": [\"_type\", \"_rank\", \"_dst\"], \"type\": -3}, {\"props\": [\"_type\", \"_rank\", \"_dst\"], \"type\": -4}]" } |
      | 6  | Argument       |              |                                                                                                                                                                                                                                |

  # The schema id is not fixed in standalone cluster, so we skip it
  @distonly
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
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                                        |
      | 17 | TopN           | 13           |                                                                                                                                                                                      |
      | 13 | Project        | 12           |                                                                                                                                                                                      |
      | 12 | BiLeftJoin     | 19, 11       |                                                                                                                                                                                      |
      | 19 | Project        | 5            |                                                                                                                                                                                      |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                                 |
      | 4  | Traverse       | 2            |                                                                                                                                                                                      |
      | 2  | Dedup          | 1            |                                                                                                                                                                                      |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                      |
      | 3  | Start          |              |                                                                                                                                                                                      |
      | 11 | Project        | 10           |                                                                                                                                                                                      |
      | 10 | AppendVertices | 9            | {  "props": "[{\"props\":[\"name\", \"age\", \"_tag\"],\"tagId\":9}, {\"props\":[\"name\", \"speciality\", \"_tag\"],\"tagId\":8}, {\"props\":[\"name\", \"_tag\"],\"tagId\":10}]" } |
      | 9  | Traverse       | 8            | {  "vertexProps": "[{\"props\":[\"name\"],\"tagId\":9}]" }                                                                                                                           |
      | 8  | Argument       |              |                                                                                                                                                                                      |
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
    And the execution plan should be:
      | id | name           | dependencies | operator info                                        |
      | 12 | Aggregate      | 13           |                                                      |
      | 13 | BiInnerJoin    | 15, 11       |                                                      |
      | 15 | Project        | 4            |                                                      |
      | 4  | Traverse       | 3            | { "vertexProps": "" }                                |
      | 3  | Traverse       | 14           | {  "vertexProps": "" }                               |
      | 14 | IndexScan      | 2            |                                                      |
      | 2  | Start          |              |                                                      |
      | 11 | Project        | 10           |                                                      |
      | 10 | AppendVertices | 9            | {  "props": "[{\"props\":[\"_tag\"],\"tagId\":9}]" } |
      | 9  | Traverse       | 8            | {  "vertexProps": "" }                               |
      | 8  | Argument       |              |                                                      |

  @distonly
  Scenario: return function
    When profiling query:
      """
      MATCH (v1)-[e:like*1..5]->(v2)
      WHERE id(v1) == "Tim Duncan"
      RETURN count(v2.player.age)
      """
    Then the result should be, in order:
      | count(v2.player.age) |
      | 24                   |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                      |
      | 7  | Aggregate      | 6            |                                                                                                    |
      | 6  | Project        | 5            |                                                                                                    |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"age\"],\"tagId\":9}]" }                                                |
      | 4  | Traverse       | 2            | {"vertexProps": "", "edgeProps": "[{\"type\": 3, \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 2  | Dedup          | 1            |                                                                                                    |
      | 1  | PassThrough    | 3            |                                                                                                    |
      | 3  | Start          |              |                                                                                                    |
    When profiling query:
      """
      MATCH (v1)-[e:like*1..5]->(v2)
      WHERE id(v1) == "Tim Duncan"
      RETURN count(v2)
      """
    Then the result should be, in order:
      | count(v2) |
      | 24        |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                      |
      | 7  | Aggregate      | 6            |                                                                                                    |
      | 6  | Project        | 5            |                                                                                                    |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"_tag\"],\"tagId\":9}]" }                                               |
      | 4  | Traverse       | 2            | {"vertexProps": "", "edgeProps": "[{\"type\": 3, \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 2  | Dedup          | 1            |                                                                                                    |
      | 1  | PassThrough    | 3            |                                                                                                    |
      | 3  | Start          |              |                                                                                                    |
    When profiling query:
      """
      MATCH p = (v1)-[e:like*1..5]->(v2)
      WHERE id(v1) == "Tim Duncan"
      RETURN length(p) LIMIT 1
      """
    Then the result should be, in order:
      | length(p) |
      | 1         |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                      |
      | 13 | Project        | 11           |                                                                                                    |
      | 11 | Limit          | 5            |                                                                                                    |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"_tag\"],\"tagId\":9}]" }                                               |
      | 4  | Traverse       | 2            | {"vertexProps": "", "edgeProps": "[{\"type\": 3, \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 2  | Dedup          | 1            |                                                                                                    |
      | 1  | PassThrough    | 3            |                                                                                                    |
      | 3  | Start          |              |                                                                                                    |
    When profiling query:
      """
      MATCH p = (a:player)-[e:like*1..3]->(b:player{age:39})
        WHERE id(a) == 'Yao Ming'
      WITH b, length(p) AS distance
      MATCH (b)-[:serve]->(t:team)
      RETURN b.player.name, distance
      """
    Then the result should be, in order:
      | b.player.name   | distance |
      | "Tracy McGrady" | 1        |
      | "Tracy McGrady" | 3        |
      | "Tracy McGrady" | 1        |
      | "Tracy McGrady" | 3        |
      | "Tracy McGrady" | 1        |
      | "Tracy McGrady" | 3        |
      | "Tracy McGrady" | 1        |
      | "Tracy McGrady" | 3        |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                  |
      | 14 | Project        | 13           |                                                                                                                                                |
      | 13 | BiInnerJoin    | 15,12        |                                                                                                                                                |
      | 15 | Project        | 17           |                                                                                                                                                |
      | 17 | AppendVertices | 16           | {  "props": "[{\"props\":[\"name\",\"age\"],\"tagId\":9}]" }                                                                                   |
      | 16 | Traverse       | 2            | {"vertexProps": "", "edgeProps": "[{\"type\": 3, \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  }                                             |
      | 2  | Dedup          | 1            |                                                                                                                                                |
      | 1  | PassThrough    | 3            |                                                                                                                                                |
      | 3  | Start          |              |                                                                                                                                                |
      | 12 | Project        | 18           |                                                                                                                                                |
      | 18 | AppendVertices | 10           | {  "props": "[{\"props\":[\"_tag\"],\"tagId\":10}]" }                                                                                          |
      | 10 | Traverse       | 8            | {"vertexProps": "[{\"props\":[\"name\",\"age\"],\"tagId\":9}]", "edgeProps": "[{\"type\": 4, \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 8  | Argument       |              |                                                                                                                                                |
      | 9  | Start          |              |                                                                                                                                                |

  @distonly
  Scenario: union match
    When profiling query:
      """
      MATCH (v:player{name:'Tim Duncan'})-[:like]->(b) RETURN id(b)
      UNION
      MATCH (a:player{age:36})-[:like]-(b) RETURN id(b)
      """
    Then the result should be, in any order:
      | id(b)               |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Marco Belinelli"   |
      | "Dejounte Murray"   |
      | "Tim Duncan"        |
      | "Boris Diaw"        |
      | "LaMarcus Aldridge" |
      | "Steve Nash"        |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                   |
      | 15 | DataCollect    | 14           |                                                                                                                                                                 |
      | 14 | Dedup          | 13           |                                                                                                                                                                 |
      | 13 | Union          | 18, 19       |                                                                                                                                                                 |
      | 18 | Project        | 4            |                                                                                                                                                                 |
      | 4  | AppendVertices | 20           | {  "props": "[{\"props\":[\"_tag\"],\"tagId\":9}]" }                                                                                                            |
      | 20 | Traverse       | 16           | {"vertexProps": "", "edgeProps": "[{\"type\": 3, \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  }                                                              |
      | 16 | IndexScan      | 2            |                                                                                                                                                                 |
      | 2  | Start          |              |                                                                                                                                                                 |
      | 19 | Project        | 10           |                                                                                                                                                                 |
      | 10 | AppendVertices | 21           | {  "props": "[{\"props\":[\"_tag\"],\"tagId\":9}]" }                                                                                                            |
      | 21 | Traverse       | 17           | {"vertexProps": "", "edgeProps": "[{\"type\": 3, \"props\": [\"_type\", \"_rank\", \"_dst\"]}, {\"type\": -3, \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 17 | IndexScan      | 8            |                                                                                                                                                                 |
      | 8  | Start          |              |                                                                                                                                                                 |

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Prune Properties rule

  # The schema id is not fixed in standalone cluster, so we skip it
  @distonly
  Scenario: Single Match
    Given a graph with space named "nba"
    When profiling query:
      """
      MATCH p = (v:player{name: "Tony Parker"})-[e:like]->(v2)
      RETURN v2.player.age
      """
    # Since in distributed system and scatter gather model, the order is not guaranteed.
    Then the result should be, in any order:
      | v2.player.age |
      | 42            |
      | 33            |
      | 41            |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                         |
      | 8  | Project        | 4            |                                                                                       |
      | 4  | AppendVertices | 3            | {  "props": "[{\"props\":[\"age\"]}]" }                                               |
      | 3  | Traverse       | 7            | {  "vertexProps": "", "edgeProps": "[{\"props\":[\"_dst\", \"_rank\", \"_type\"]}]" } |
      | 7  | IndexScan      | 2            |                                                                                       |
      | 2  | Start          |              |                                                                                       |
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
      | id | name           | dependencies | operator info                                                                                                 |
      | 8  | Project        | 4            |                                                                                                               |
      | 4  | AppendVertices | 3            | {  "props": "[{ \"props\": [\"_tag\"]}, {\"props\": [\"_tag\"]}, {\"props\": [\"_tag\"]}]"}                   |
      | 3  | Traverse       | 7            | {  "vertexProps": "[{\"props\":[\"name\"]}]", "edgeProps": "[{\"props\":[\"_dst\", \"_rank\", \"_type\"]}]" } |
      | 7  | IndexScan      | 2            |                                                                                                               |
      | 2  | Start          |              |                                                                                                               |
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
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                                          |
      | 8  | Project        | 4            |                                                                                                                                                                                                                                                                        |
      | 4  | AppendVertices | 3            | {  "props": "[{\"props\": [\"_tag\"]}, {\"props\": [\"_tag\"]}, {\"props\": [\"_tag\"]}]" }                                                                                                                                                                            |
      | 3  | Traverse       | 7            | { "vertexProps": "[{\"props\": [\"name\", \"age\", \"_tag\"]}, {\"props\": [\"name\", \"speciality\", \"_tag\"]}, {\"props\": [\"name\", \"_tag\"]}]", "edgeProps": "[{\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}]" } |
      | 7  | IndexScan      | 2            |                                                                                                                                                                                                                                                                        |
      | 2  | Start          |              |                                                                                                                                                                                                                                                                        |
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
      | id | name           | dependencies | operator info                                                                                                                                   |
      | 8  | Project        | 4            |                                                                                                                                                 |
      | 4  | AppendVertices | 3            | {  "props": "[{\"props\":[\"name\", \"age\", \"_tag\"]}, {\"props\":[\"name\", \"speciality\", \"_tag\"]}, {\"props\":[\"name\", \"_tag\"]}]" } |
      | 3  | Traverse       | 7            | {  "vertexProps": "", "edgeProps": "[{\"props\":[\"_dst\", \"_rank\", \"_type\"]}]" }                                                           |
      | 7  | IndexScan      | 2            |                                                                                                                                                 |
      | 2  | Start          |              |                                                                                                                                                 |
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
      | id | name           | dependencies | operator info                                                                             |
      | 8  | Project        | 4            |                                                                                           |
      | 4  | AppendVertices | 3            | {  "props": "[{\"props\":[\"_tag\"]}, {\"props\":[\"_tag\"]}, {\"props\":[\"_tag\"]} ]" } |
      | 3  | Traverse       | 7            | {  "vertexProps": "", "edgeProps": "[{\"props\":[\"_dst\", \"_rank\", \"_type\"]}]" }     |
      | 7  | IndexScan      | 2            |                                                                                           |
      | 2  | Start          |              |                                                                                           |
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
    Given a graph with space named "nba"
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
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                                                                                                                                                                                  |
      | 16 | TopN           | 12           |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 12 | Project        | 9            |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 9  | HashInnerJoin  | 22, 23       |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 22 | Project        | 5            |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                                                                       |
      | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"] }]" }                                                                                                                                                                                                                                                                                                                                                                |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 3  | Start          |              |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 23 | Project        | 8            |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 8  | AppendVertices | 7            | {  "props": "[{ \"props\":[\"name\"]}, { \"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                                             |
      | 7  | Traverse       | 6            | { "vertexProps": "[{ \"props\":[\"name\"]}]", "edgeProps": "[{  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}]" } |
      | 6  | Argument       | 0            |                                                                                                                                                                                                                                                                                                                                                                                                                |
      | 0  | Start          |              |                                                                                                                                                                                                                                                                                                                                                                                                                |
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
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                                                                                                                                    |
      | 20 | TopN           | 23           |                                                                                                                                                                                                                                                                                                                                                                  |
      | 23 | Project        | 13           |                                                                                                                                                                                                                                                                                                                                                                  |
      | 13 | HashInnerJoin  | 9, 30        |                                                                                                                                                                                                                                                                                                                                                                  |
      | 9  | HashInnerJoin  | 28, 29       |                                                                                                                                                                                                                                                                                                                                                                  |
      | 28 | Project        | 5            |                                                                                                                                                                                                                                                                                                                                                                  |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"] }]" }                                                                                                                                                                                                                                                                                                                        |
      | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"] }]", "edgeProps": "[{  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}]" } |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                                                                                                                                                                  |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                                                                                                                                                  |
      | 3  | Start          |              |                                                                                                                                                                                                                                                                                                                                                                  |
      | 29 | Project        | 8            |                                                                                                                                                                                                                                                                                                                                                                  |
      | 8  | AppendVertices | 7            | {  "props": "[{ \"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                        |
      | 7  | Traverse       | 6            | { "vertexProps": "[{ \"props\":[\"name\"]}]", "edgeProps": "[{  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}]" }  |
      | 6  | Argument       |              |                                                                                                                                                                                                                                                                                                                                                                  |
      | 30 | Project        | 12           |                                                                                                                                                                                                                                                                                                                                                                  |
      | 12 | AppendVertices | 11           | {  "props": "[{\"props\":[\"name\"] }]" }                                                                                                                                                                                                                                                                                                                        |
      | 11 | Traverse       | 10           | {  "vertexProps": "[{\"props\":[\"name\"] }]", "edgeProps": "[{  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"]}]" } |
      | 10 | Argument       | 0            |                                                                                                                                                                                                                                                                                                                                                                  |
      | 0  | Start          |              |                                                                                                                                                                                                                                                                                                                                                                  |

  # The schema id is not fixed in standalone cluster, so we skip it
  @distonly
  Scenario: Multi Match
    Given a graph with space named "nba"
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
      | id | name           | dependencies | operator info                                                      |
      | 17 | TopN           | 13           |                                                                    |
      | 13 | Project        | 12           |                                                                    |
      | 12 | HashInnerJoin  | 19, 11       |                                                                    |
      | 19 | Project        | 5            |                                                                    |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"] }]" }                          |
      | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"] }]" }                    |
      | 2  | Dedup          | 1            |                                                                    |
      | 1  | PassThrough    | 3            |                                                                    |
      | 3  | Start          |              |                                                                    |
      | 11 | Project        | 10           |                                                                    |
      | 10 | AppendVertices | 9            | {  "props": "[{ \"props\":[\"name\"]}, { \"props\":[\"name\"]}]" } |
      | 9  | Traverse       | 8            | { "vertexProps": "[{ \"props\":[\"name\"]}]" }                     |
      | 8  | Argument       |              |                                                                    |
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
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                                                                                                                                   |
      | 21 | TopN           | 17           |                                                                                                                                                                                                                                                                                                                                                                 |
      | 17 | Project        | 16           |                                                                                                                                                                                                                                                                                                                                                                 |
      | 16 | HashInnerJoin  | 23, 14       |                                                                                                                                                                                                                                                                                                                                                                 |
      | 23 | Project        | 5            |                                                                                                                                                                                                                                                                                                                                                                 |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"] }]" }                                                                                                                                                                                                                                                                                                                       |
      | 4  | Traverse       | 2            | {  "vertexProps": "[{\"props\":[\"name\"] }]" }                                                                                                                                                                                                                                                                                                                 |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                                                                                                                                                                 |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                                                                                                                                                 |
      | 3  | Start          |              |                                                                                                                                                                                                                                                                                                                                                                 |
      | 14 | HashInnerJoin  | 33, 34       |                                                                                                                                                                                                                                                                                                                                                                 |
      | 33 | Project        | 10           |                                                                                                                                                                                                                                                                                                                                                                 |
      | 10 | AppendVertices | 9            | {  "props": "[{ \"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                       |
      | 9  | Traverse       | 8            | { "vertexProps": "[{ \"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                  |
      | 8  | Argument       |              |                                                                                                                                                                                                                                                                                                                                                                 |
      | 34 | Project        | 13           |                                                                                                                                                                                                                                                                                                                                                                 |
      | 13 | AppendVertices | 12           | {  "props": "[{ \"props\":[\"name\"]}]" }                                                                                                                                                                                                                                                                                                                       |
      | 12 | Traverse       | 11           | { "vertexProps": "[{ \"props\":[\"name\"]}]", "edgeProps": "[{\"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"] }, {  \"props\": [\"_dst\", \"_rank\", \"_type\"]}, {\"props\": [\"_dst\", \"_rank\", \"_type\"] }, {\"props\": [\"_dst\", \"_rank\", \"_type\"] }, {\"props\": [\"_dst\", \"_rank\", \"_type\"] }]" } |
      | 11 | Argument       |              |                                                                                                                                                                                                                                                                                                                                                                 |
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
      | id | name           | dependencies | operator info                                                                                                                                                                        |
      | 10 | Project        | 11           |                                                                                                                                                                                      |
      | 11 | HashInnerJoin  | 14, 9        |                                                                                                                                                                                      |
      | 14 | Project        | 3            |                                                                                                                                                                                      |
      | 3  | AppendVertices | 12           | {  "props": "[{\"props\":[\"name\"] }]" }                                                                                                                                            |
      | 12 | IndexScan      | 2            |                                                                                                                                                                                      |
      | 2  | Start          |              |                                                                                                                                                                                      |
      | 9  | Project        | 8            |                                                                                                                                                                                      |
      | 8  | AppendVertices | 7            | {  "props": "[{\"props\":[\"name\"] }]" }                                                                                                                                            |
      | 7  | Traverse       | 6            | {  "vertexProps": "", "edgeProps": "[{\"props\": [\"_type\", \"_rank\", \"_dst\"]}, {\"props\": [\"_type\", \"_rank\", \"_dst\"]}, {\"props\": [\"_type\", \"_rank\", \"_dst\"] }]"} |
      | 6  | Argument       | 0            |                                                                                                                                                                                      |
      | 0  | Start          |              |                                                                                                                                                                                      |

  Scenario: Project after aggregate
    Given a graph with space named "nba"
    When profiling query:
      """
      MATCH (v1:player)-[e:like]->(v2:player)
      WHERE id(v1) == "Tony Parker"
      WITH v1, v2, count(e.likeness) AS cnt
      RETURN v1.player.age, v2.player.age, cnt
      """
    Then the result should be, in any order:
      | v1.player.age | v2.player.age | cnt |
      | 36            | 42            | 1   |
      | 36            | 41            | 1   |
      | 36            | 33            | 1   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                                                                                                                         |
      | 8  | Project        | 7            |                |                                                                                                                                       |
      | 7  | Aggregate      | 6            |                |                                                                                                                                       |
      | 6  | Project        | 10           |                |                                                                                                                                       |
      | 10 | AppendVertices | 9            |                | {  "props": "[{ \"props\":[\"age\", \"_tag\"]}]" }                                                                                    |
      | 9  | Traverse       | 2            |                | { "vertexProps": "[{ \"props\":[\"age\"]}]", "edgeProps": "[{\"props\": [\"_src\", \"_dst\", \"_rank\", \"_type\", \"likeness\"]}]" } |
      | 2  | Dedup          | 1            |                |                                                                                                                                       |
      | 1  | PassThrough    | 3            |                |                                                                                                                                       |
      | 3  | Start          |              |                |                                                                                                                                       |

  # The schema id is not fixed in standalone cluster, so we skip it
  @distonly
  Scenario: Optional Match
    Given a graph with space named "nba"
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
      | id | name           | dependencies | operator info                                                                                                                                      |
      | 17 | TopN           | 13           |                                                                                                                                                    |
      | 13 | Project        | 12           |                                                                                                                                                    |
      | 12 | HashLeftJoin   | 19, 11       |                                                                                                                                                    |
      | 19 | Project        | 5            |                                                                                                                                                    |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"name\"] }]" }                                                                                                          |
      | 4  | Traverse       | 2            |                                                                                                                                                    |
      | 2  | Dedup          | 1            |                                                                                                                                                    |
      | 1  | PassThrough    | 3            |                                                                                                                                                    |
      | 3  | Start          |              |                                                                                                                                                    |
      | 11 | Project        | 10           |                                                                                                                                                    |
      | 10 | AppendVertices | 9            | {  "props": "[{\"props\":[\"name\", \"age\", \"_tag\"] }, {\"props\":[\"name\", \"speciality\", \"_tag\"] }, {\"props\":[\"name\", \"_tag\"] }]" } |
      | 9  | Traverse       | 8            | {  "vertexProps": "[{\"props\":[\"name\"] }]" }                                                                                                    |
      | 8  | Argument       |              |                                                                                                                                                    |
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
      | id | name           | dependencies | operator info          |
      | 12 | Aggregate      | 13           |                        |
      | 13 | HashInnerJoin  | 15, 11       |                        |
      | 15 | Project        | 5            |                        |
      | 5  | AppendVertices | 4            |                        |
      | 4  | Traverse       | 3            | { "vertexProps": "" }  |
      | 3  | Traverse       | 14           | {  "vertexProps": "" } |
      | 14 | IndexScan      | 2            |                        |
      | 2  | Start          |              |                        |
      | 11 | Project        | 9            |                        |
      | 9  | Traverse       | 8            |                        |
      | 8  | Argument       |              |                        |

  @distonly
  Scenario: return function
    Given a graph with space named "nba"
    When profiling query:
      """
      MATCH (v1)-[:like]->(v2)
      WHERE id(v1) == "Tim Duncan"
      RETURN count(v2), v1
      """
    Then the result should be, in order:
      | count(v2) | v1                                                                                                          |
      | 2         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                                                                                                              |
      | 7  | Aggregate      | 6            |                                                                                                                                                                                                                            |
      | 6  | Project        | 5            |                                                                                                                                                                                                                            |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"_tag\"] }, {\"props\":[\"_tag\"] }, {\"props\":[\"_tag\"] }]" }                                                                                                                                |
      | 4  | Traverse       | 2            | {"vertexProps": "[{\"props\":[\"name\", \"age\", \"_tag\"] }, {\"props\":[\"name\", \"speciality\", \"_tag\"] }, {\"props\":[\"name\", \"_tag\"] }]" , "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                            |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                            |
      | 3  | Start          |              |                                                                                                                                                                                                                            |
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
      | id | name           | dependencies | operator info                                                                           |
      | 7  | Aggregate      | 8            |                                                                                         |
      | 8  | AppendVertices | 4            | {  "props": "[{\"props\":[\"age\"] }]" }                                                |
      | 4  | Traverse       | 2            | {"vertexProps": "", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 2  | Dedup          | 1            |                                                                                         |
      | 1  | PassThrough    | 3            |                                                                                         |
      | 3  | Start          |              |                                                                                         |
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
      | id | name           | dependencies | operator info                                                                               |
      | 7  | Aggregate      | 8            |                                                                                             |
      | 8  | AppendVertices | 4            | {  "props": "[{\"props\":[\"_tag\"] }, {\"props\":[\"_tag\"] }, {\"props\":[\"_tag\"] }]" } |
      | 4  | Traverse       | 2            | {"vertexProps": "" , "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  }    |
      | 2  | Dedup          | 1            |                                                                                             |
      | 1  | PassThrough    | 3            |                                                                                             |
      | 3  | Start          |              |                                                                                             |
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
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                     |
      | 13 | Project        | 11           |                                                                                                                                                                                                                                                   |
      | 11 | Limit          | 5            |                                                                                                                                                                                                                                                   |
      | 5  | AppendVertices | 4            | {  "props": "[{\"props\":[\"_tag\", \"name\", \"speciality\"] }, {\"props\":[\"_tag\", \"name\", \"age\"] }, {\"props\":[\"_tag\", \"name\"] }]" }                                                                                                |
      | 4  | Traverse       | 2            | {"vertexProps": "[{\"props\":[\"name\", \"age\", \"_tag\"] }, {\"props\":[\"name\", \"speciality\", \"_tag\"] }, {\"props\":[\"name\", \"_tag\"] }]", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\", \"_src\", \"likeness\"]}]"  } |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                                                   |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                                   |
      | 3  | Start          |              |                                                                                                                                                                                                                                                   |
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
      | id | name           | dependencies | operator info                                                                                                                                                                                                                                     |
      | 14 | Project        | 13           |                                                                                                                                                                                                                                                   |
      | 13 | HashInnerJoin  | 15,12        |                                                                                                                                                                                                                                                   |
      | 15 | Project        | 17           |                                                                                                                                                                                                                                                   |
      | 17 | AppendVertices | 16           | {  "props": "[{\"props\":[\"_tag\", \"name\", \"speciality\"] }, {\"props\":[\"_tag\", \"name\", \"age\"] }, {\"props\":[\"_tag\", \"name\"] }]" }                                                                                                |
      | 16 | Traverse       | 2            | {"vertexProps": "[{\"props\":[\"name\", \"age\", \"_tag\"] }, {\"props\":[\"name\", \"speciality\", \"_tag\"] }, {\"props\":[\"name\", \"_tag\"] }]", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\", \"_src\", \"likeness\"]}]"  } |
      | 2  | Dedup          | 1            |                                                                                                                                                                                                                                                   |
      | 1  | PassThrough    | 3            |                                                                                                                                                                                                                                                   |
      | 3  | Start          |              |                                                                                                                                                                                                                                                   |
      | 12 | Project        | 18           |                                                                                                                                                                                                                                                   |
      | 18 | AppendVertices | 10           | {  "props": "[{\"props\":[\"_tag\"] }]" }                                                                                                                                                                                                         |
      | 10 | Traverse       | 8            | {"vertexProps": "[{\"props\":[\"name\", \"age\", \"_tag\"] }, {\"props\":[\"name\", \"speciality\", \"_tag\"] }, {\"props\":[\"name\", \"_tag\"] }]", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  }                         |
      | 8  | Argument       |              |                                                                                                                                                                                                                                                   |

  @distonly
  Scenario: union match
    Given a graph with space named "nba"
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
      | id | name           | dependencies | operator info                                                                                                                            |
      | 14 | Dedup          | 13           |                                                                                                                                          |
      | 13 | Union          | 18, 19       |                                                                                                                                          |
      | 18 | Project        | 4            |                                                                                                                                          |
      | 4  | AppendVertices | 20           | {  "props": "[{\"props\":[\"_tag\"] }, {\"props\":[\"_tag\"] }, {\"props\":[\"_tag\"] }]" }                                              |
      | 20 | Traverse       | 16           | {"vertexProps": "", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  }                                                  |
      | 16 | IndexScan      | 2            |                                                                                                                                          |
      | 2  | Start          |              |                                                                                                                                          |
      | 19 | Project        | 10           |                                                                                                                                          |
      | 10 | AppendVertices | 21           | {  "props": "[{\"props\":[\"_tag\"] }, {\"props\":[\"_tag\"] }, {\"props\":[\"_tag\"] }]" }                                              |
      | 21 | Traverse       | 17           | {"vertexProps": "", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}, {  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 17 | IndexScan      | 8            |                                                                                                                                          |
      | 8  | Start          |              |                                                                                                                                          |

  @distonly
  Scenario: optional match
    Given a graph with space named "nba"
    When profiling query:
      """
      MATCH (v:player)-[:like]-(:player)<-[:teammate]-(b:player)-[:serve]->(t:team)
        WHERE id(v) == 'Tim Duncan' AND b.player.age > 20
      WITH v, count(b) AS countB, t
      OPTIONAL MATCH (v)-[:like]-()<-[:like]-(oldB)-[:serve]->(t)
      WITH v, countB, t, count(oldB) AS cb
      RETURN t.team.name, sum(countB)
      """
    Then the result should be, in any order:
      | t.team.name | sum(countB) |
      | "Spurs"     | 11          |
      | "Hornets"   | 3           |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                            |
      | 19 | Aggregate      | 18           |                                                                                                                                          |
      | 18 | Aggregate      | 27           |                                                                                                                                          |
      | 27 | HashLeftJoin   | 10,26        |                                                                                                                                          |
      | 10 | Aggregate      | 21           |                                                                                                                                          |
      | 21 | Project        | 20           |                                                                                                                                          |
      | 20 | Filter         | 25           |                                                                                                                                          |
      | 25 | AppendVertices | 24           | {  "props": "[{ \"props\":[\"name\",\"_tag\"]}]" }                                                                                       |
      | 24 | Filter         | 24           |                                                                                                                                          |
      | 24 | Traverse       | 23           | {"vertexProps": "[{ \"props\":[\"age\"]}]", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  }                          |
      | 23 | Traverse       | 22           | {"vertexProps": "", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  }                                                  |
      | 22 | Traverse       | 2            | {"vertexProps": "", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}, {  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 2  | Dedup          | 1            |                                                                                                                                          |
      | 1  | PassThrough    | 3            |                                                                                                                                          |
      | 3  | Start          |              |                                                                                                                                          |
      | 26 | Project        | 14           |                                                                                                                                          |
      | 14 | Traverse       | 13           | {"edgeProps": "[{  \"props\": [\"_src\", \"_type\", \"_rank\", \"_dst\", \"start_year\", \"end_year\"]}]"  }                             |
      | 13 | Traverse       | 12           | {"vertexProps": "", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  }                                                  |
      | 12 | Traverse       | 11           | {"vertexProps": "", "edgeProps": "[{  \"props\": [\"_type\", \"_rank\", \"_dst\"]}, {  \"props\": [\"_type\", \"_rank\", \"_dst\"]}]"  } |
      | 11 | Argument       |              |                                                                                                                                          |

  @distonly
  Scenario: test properties:
    Given an empty graph
    And load "nba" csv data to a new space
    And having executed:
      """
      ALTER TAG player ADD (sex string NOT NULL DEFAULT "男");
      ALTER EDGE like ADD (start_year int64 NOT NULL DEFAULT 2022);
      ALTER EDGE serve ADD (degree int64 NOT NULL DEFAULT 88);
      """
    And wait 6 seconds
    When executing query:
      """
      match (v:player) return properties(v).name AS name order by name limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | name                |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
    When executing query:
      """
      match (v:player) return properties(v).name AS name, v.player.age AS age order by name limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | name                | age |
      | "Amar'e Stoudemire" | 36  |
      | "Aron Baynes"       | 32  |
    When executing query:
      """
      match (v:player) where properties(v).name ==  "LaMarcus Aldridge" return properties(v).age;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v).age |
      | 33                |
    When executing query:
      """
      match (v:player) where properties(v).name ==  "LaMarcus Aldridge" return v.player.age;
      """
    Then the result should be, in order, with relax comparison:
      | v.player.age |
      | 33           |
    When executing query:
      """
      match (v:player) where properties(v).name=="LaMarcus Aldridge" return v.player.sex,properties(v).age;
      """
    Then the result should be, in order, with relax comparison:
      | v.player.sex | properties(v).age |
      | "男"         | 33                |
    When executing query:
      """
      match (v:player) where id(v)=="Carmelo Anthony" return v.player.age;
      """
    Then the result should be, in order, with relax comparison:
      | v.player.age |
      | 34           |
    When executing query:
      """
      match (v:player) where id(v)=="Carmelo Anthony" return properties(v).age;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v).age |
      | 34                |
    When executing query:
      """
      match (v:player) where id(v)=="Carmelo Anthony" return properties(v).age,v.player.sex;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v).age | v.player.sex |
      | 34                | "男"         |
    When executing query:
      """
      match (v:player{name:"LaMarcus Aldridge"}) return v.player.age;
      """
    Then the result should be, in order, with relax comparison:
      | v.player.age |
      | 33           |
    When executing query:
      """
      match (v:player{name:"LaMarcus Aldridge"}) return properties(v).age;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v).age |
      | 33                |
    When executing query:
      """
      match (v:player{name:"LaMarcus Aldridge"}) return properties(v).age,v.player.sex;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v).age | v.player.sex |
      | 33                | "男"         |
    When executing query:
      """
      match (v:player) return id(v) AS id, properties(v).name AS name, v.player.age AS age order by name limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | id                  | name                | age |
      | "Amar'e Stoudemire" | "Amar'e Stoudemire" | 36  |
      | "Aron Baynes"       | "Aron Baynes"       | 32  |
    When executing query:
      """
      match (v)-[]->(b:player) return id(v) AS id, properties(v).name AS name, v.player.age AS age order by name limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | id                  | name                | age |
      | "Amar'e Stoudemire" | "Amar'e Stoudemire" | 36  |
      | "Aron Baynes"       | "Aron Baynes"       | 32  |
    When executing query:
      """
      match ()-[e:serve]->() return properties(e).start_year AS year order by year limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | year |
      | 1992 |
      | 1994 |
    When executing query:
      """
      match ()-[e:serve]->() return properties(e).start_year AS year,e.end_year  order by year limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | year | e.end_year |
      | 1992 | 1996       |
      | 1994 | 1996       |
    When executing query:
      """
      match ()-[e:serve]->() where e.start_year>1022 return properties(e).end_year AS year order by year limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | year |
      | 1996 |
      | 1996 |
    When executing query:
      """
      match ()-[e:serve]->() where e.start_year>1022 return e.end_year AS year order by year limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | year |
      | 1996 |
      | 1996 |
    When executing query:
      """
      match ()-[e:serve]->() where e.start_year>1022 return e.end_year AS year, properties(e).degree order by year limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | year | properties(e).degree |
      | 1996 | 88                   |
      | 1996 | 88                   |
    When executing query:
      """
      match ()-[e:serve]->() where e.start_year>1022 return properties(e).degree limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | properties(e).degree |
      | 88                   |
      | 88                   |
    When executing query:
      """
      match ()-[e:serve{degree:88}]->()  return properties(e).start_year AS year order by year limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | year |
      | 1992 |
      | 1994 |
    When executing query:
      """
      match ()-[e:serve{degree:88}]->()  return e.end_year AS year order by year limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | year |
      | 1996 |
      | 1996 |
    When executing query:
      """
      match ()-[e:serve{degree:88}]->()  return e.end_year, properties(e).start_year AS year order by year limit 2;
      """
    Then the result should be, in order, with relax comparison:
      | e.end_year | year |
      | 1996       | 1992 |
      | 1996       | 1994 |
    When executing query:
      """
      match (src_v:player{name:"Manu Ginobili"})-[e]-(dst_v)
        return properties(src_v).age,properties(e).degree,properties(dst_v).name AS name,src_v.player.sex,e.start_year,dst_v.player.age
        order by name limit 3;
      """
    Then the result should be, in order, with relax comparison:
      | properties(src_v).age | properties(e).degree | name              | src_v.player.sex | e.start_year | dst_v.player.age |
      | 41                    | NULL                 | "Dejounte Murray" | "男"             | 2022         | 29               |
      | 41                    | 88                   | "Spurs"           | "男"             | 2002         | NULL             |
      | 41                    | NULL                 | "Tiago Splitter"  | "男"             | 2022         | 34               |
    When executing query:
      """
      match (src_v:player{name:"Manu Ginobili"})-[e*2]-(dst_v)
        return properties(src_v).sex,properties(e[0]).degree as degree,properties(dst_v).name as name,src_v.player.age AS age, e[1].start_year,dst_v.player.age
        order by degree, name, age limit 5;
      """
    Then the result should be, in order, with relax comparison:
      | properties(src_v).sex | degree | name          | age | e[1].start_year | dst_v.player.age |
      | "男"                  | 88     | "Aron Baynes" | 41  | 2013            | 32               |
      | "男"                  | 88     | "Boris Diaw"  | 41  | 2012            | 36               |
      | "男"                  | 88     | "Cory Joseph" | 41  | 2011            | 27               |
      | "男"                  | 88     | "Danny Green" | 41  | 2010            | 31               |
      | "男"                  | 88     | "David West"  | 41  | 2015            | 38               |
    When executing query:
      """
      match (src_v:player{name:"Manu Ginobili"})-[e:like*2..3]-(dst_v)
        return distinct properties(src_v).sex,properties(e[0]).degree as degree,properties(dst_v).name as name,src_v.player.age AS age, e[1].start_year,dst_v.player.age
        order by degree, name, age limit 5;
      """
    Then the result should be, in order, with relax comparison:
      | properties(src_v).sex | degree | name              | age | e[1].start_year | dst_v.player.age |
      | "男"                  | NULL   | "Aron Baynes"     | 41  | 2022            | 32               |
      | "男"                  | NULL   | "Blake Griffin"   | 41  | 2022            | 30               |
      | "男"                  | NULL   | "Boris Diaw"      | 41  | 2022            | 36               |
      | "男"                  | NULL   | "Carmelo Anthony" | 41  | 2022            | 34               |
      | "男"                  | NULL   | "Chris Paul"      | 41  | 2022            | 33               |
    When executing query:
      """
      match (v1)-->(v2)-->(v3) where id(v1)=="Manu Ginobili"
        return properties(v1).name,properties(v2).age AS age,properties(v3).name as name
        order by age, name limit 1;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v1).name | age | name      |
      | "Manu Ginobili"     | 36  | "Hornets" |
    When executing query:
      """
      match (v1)-->(v2)-->(v3) where id(v1)=="Manu Ginobili"
        return properties(v1).name,properties(v2).age AS age,properties(v3).name as name,v1.player.sex,v2.player.sex,id(v3)
        order by age, name limit 1;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v1).name | age | name      | v1.player.sex | v2.player.sex | id(v3)    |
      | "Manu Ginobili"     | 36  | "Hornets" | "男"          | "男"          | "Hornets" |
    When executing query:
      """
      match (v1)-->(v2:player)-->(v3) where v2.player.name=="Shaquille O'Neal"
        return properties(v1).name,properties(v2).age as age,properties(v3).name AS name,v2.player.sex,v1.player.age
        order by age, name limit 1;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v1).name | age | name        | v2.player.sex | v1.player.age |
      | "Yao Ming"          | 47  | "Cavaliers" | "男"          | 38            |
    When executing query:
      """
      match (v1)-->(v2:player)-->(v3) where v2.player.name=="Shaquille O'Neal"
        return properties(v1).name,properties(v2).age as age,properties(v3).name AS name
        order by name limit 1;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v1).name | age | name        |
      | "Yao Ming"          | 47  | "Cavaliers" |
    When executing query:
      """
      match (v1)-->(v2)-->(v3:team{name:"Celtics"})
        return properties(v1).name,properties(v2).age as age,properties(v3).name AS name
        order by name, age limit 1;
      """
    Then the result should be, in order, with relax comparison:
      | properties(v1).name | age | name      |
      | "Ray Allen"         | 33  | "Celtics" |
    When executing query:
      """
      match (src_v)-[e:like|serve]->(dst_v) where  id(src_v)=="Rajon Rondo"
        return properties(e).degree, e.end_year AS year
        order by year limit 3;
      """
    Then the result should be, in order, with relax comparison:
      | properties(e).degree | year |
      | 88                   | 2014 |
      | 88                   | 2015 |
      | 88                   | 2016 |
    When executing query:
      """
      match (src_v)-[e:like|serve]->(dst_v)-[e2]-(dst_v2) where  id(src_v)=="Rajon Rondo"
        return properties(e).degree as degree,properties(e2).degree AS degree1
        order by degree, degree1 limit 5;
      """
    Then the result should be, in order, with relax comparison:
      | degree | degree1 |
      | 88     | 88      |
      | 88     | 88      |
      | 88     | 88      |
      | 88     | 88      |
      | 88     | 88      |
    When executing query:
      """
      match (src_v)-[e:like|serve]->(dst_v)-[e2]-(dst_v2) where  id(src_v)=="Rajon Rondo" return properties(e).degree1,properties(e).degree1,e2.a,dst_v.p.name,dst_v.player.sex1,properties(src_v).name2 limit 5;
      """
    Then the result should be, in order, with relax comparison:
      | properties(e).degree1 | properties(e).degree1 | e2.a | dst_v.p.name | dst_v.player.sex1 | properties(src_v).name2 |
      | NULL                  | NULL                  | NULL | NULL         | NULL              | NULL                    |
      | NULL                  | NULL                  | NULL | NULL         | NULL              | NULL                    |
      | NULL                  | NULL                  | NULL | NULL         | NULL              | NULL                    |
      | NULL                  | NULL                  | NULL | NULL         | NULL              | NULL                    |
      | NULL                  | NULL                  | NULL | NULL         | NULL              | NULL                    |
    Then drop the used space

  Scenario: Project on not exist tag
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)-[e:like]->(t) WHERE v.player.name=='Tim Duncan'  RETURN v.player.name, v.x.y, v.player.age
      """
    Then the result should be, in any order, with relax comparison:
      | v.player.name | v.x.y | v.player.age |
      | "Tim Duncan"  | NULL  | 42           |
      | "Tim Duncan"  | NULL  | 42           |
    When executing query:
      """
      MATCH (v:player)-[:like]->(t) WHERE v.player.name=="Tim Duncan" RETURN v.player.name, properties(v), t
      """
    Then the result should be, in any order, with relax comparison:
      | v.player.name | properties(v)                                           | t                                                         |
      | "Tim Duncan"  | {age: 42, name: "Tim Duncan", speciality: "psychology"} | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
      | "Tim Duncan"  | {age: 42, name: "Tim Duncan", speciality: "psychology"} | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
    When executing query:
      """
      MATCH (v:player)-[:like]->(t) WHERE v.player.name=="Tim Duncan" RETURN v.player.name, t.errortag.name, properties(v), t
      """
    Then the result should be, in any order, with relax comparison:
      | v.player.name | t.errortag.name | properties(v)                                           | t                                                         |
      | "Tim Duncan"  | NULL            | {age: 42, name: "Tim Duncan", speciality: "psychology"} | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
      | "Tim Duncan"  | NULL            | {age: 42, name: "Tim Duncan", speciality: "psychology"} | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |

  Scenario: no pruning on agg after unwind
    Given a graph with space named "nba"
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return count(a)
      """
    Then the result should be, in any order:
      | count(a) |
      | 5        |
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return sum(a)
      """
    Then the result should be, in any order:
      | sum(a) |
      | 10025  |
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return max(a)
      """
    Then the result should be, in any order:
      | max(a) |
      | 2015   |
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return min(a)
      """
    Then the result should be, in any order:
      | min(a) |
      | 1997   |
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return avg(a)
      """
    Then the result should be, in any order:
      | avg(a) |
      | 2005.0 |
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return std(a)
      """
    Then the result should be, in any order:
      | std(a)            |
      | 6.542170893518461 |
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return std(a)
      """
    Then the result should be, in any order:
      | std(a)            |
      | 6.542170893518461 |
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return bit_or(a)
      """
    Then the result should be, in any order:
      | bit_or(a) |
      | 2015      |
    When executing query:
      """
      match (v0:player)-[e0]->(v1) where id(v0) == "Tim Duncan" unwind e0.start_year as a return bit_and(a)
      """
    Then the result should be, in any order:
      | bit_and(a) |
      | 1984       |

  Scenario: Return *
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)-[e*1..2]->(v2:player) WHERE id(v) =='Yao Ming'  RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v                                               | e                                                                                                                         | v2                                                                                                          |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | [[:like "Yao Ming"->"Shaquille O'Neal" @0 {likeness: 90}]]                                                                | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | [[:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]]                                                                   | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | [[:like "Yao Ming"->"Shaquille O'Neal" @0 {likeness: 90}], [:like "Shaquille O'Neal"->"JaVale McGee" @0 {likeness: 100}]] | ("JaVale McGee" :player{age: 31, name: "JaVale McGee"})                                                     |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | [[:like "Yao Ming"->"Shaquille O'Neal" @0 {likeness: 90}], [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]]    | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | [[:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}], [:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}]]          | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | [[:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}], [:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}]]         | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | [[:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}], [:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}]]            | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |

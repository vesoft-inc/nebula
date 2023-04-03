# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Remove AppendVertices Below Join

  Background:
    Given a graph with space named "nba"

  Scenario: Remove AppendVertices below left join
    When profiling query:
      """
      MATCH (person:player)-[:like*1..2]-(friend:player)-[:serve]->(friendTeam:team)
      WHERE id(person) == "Tony Parker" AND id(friend) != "Tony Parker"
      WITH DISTINCT friend, friendTeam
      OPTIONAL MATCH (friend)<-[:like]-(friend2:player)<-[:like]-(friendTeam)
      WITH friendTeam, count(friend2) AS numFriends
      RETURN
      id(friendTeam) AS teamId,
      friendTeam.team.name AS teamName,
      numFriends
      ORDER BY teamName DESC
      """
    Then the result should be, in order, with relax comparison:
      | teamId          | teamName        | numFriends |
      | "Warriors"      | "Warriors"      | 0          |
      | "Trail Blazers" | "Trail Blazers" | 0          |
      | "Thunders"      | "Thunders"      | 0          |
      | "Suns"          | "Suns"          | 0          |
      | "Spurs"         | "Spurs"         | 0          |
      | "Rockets"       | "Rockets"       | 0          |
      | "Raptors"       | "Raptors"       | 0          |
      | "Pistons"       | "Pistons"       | 0          |
      | "Magic"         | "Magic"         | 0          |
      | "Lakers"        | "Lakers"        | 0          |
      | "Kings"         | "Kings"         | 0          |
      | "Jazz"          | "Jazz"          | 0          |
      | "Hornets"       | "Hornets"       | 0          |
      | "Heat"          | "Heat"          | 0          |
      | "Hawks"         | "Hawks"         | 0          |
      | "Grizzlies"     | "Grizzlies"     | 0          |
      | "Clippers"      | "Clippers"      | 0          |
      | "Celtics"       | "Celtics"       | 0          |
      | "Cavaliers"     | "Cavaliers"     | 0          |
      | "Bulls"         | "Bulls"         | 0          |
      | "76ers"         | "76ers"         | 0          |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                           |
      | 21 | Sort           | 18           |                                                                                                                         |
      | 18 | Project        | 17           |                                                                                                                         |
      | 17 | Aggregate      | 16           |                                                                                                                         |
      | 16 | HashLeftJoin   | 10,15        | {"hashKeys": ["_joinkey($-.friendTeam)", "_joinkey($-.friend)"], "probeKeys": ["$-.friendTeam", "_joinkey($-.friend)"]} |
      | 10 | Dedup          | 28           |                                                                                                                         |
      | 28 | Project        | 22           |                                                                                                                         |
      | 22 | Filter         | 26           |                                                                                                                         |
      | 26 | AppendVertices | 25           |                                                                                                                         |
      | 25 | Traverse       | 24           |                                                                                                                         |
      | 24 | Traverse       | 2            |                                                                                                                         |
      | 2  | Dedup          | 1            |                                                                                                                         |
      | 1  | PassThrough    | 3            |                                                                                                                         |
      | 3  | Start          |              |                                                                                                                         |
      | 15 | Project        | 14           | {"columns": ["$-.friend AS friend", "$-.friend2 AS friend2", "none_direct_dst($-.__VAR_3,$-.friend2) AS friendTeam"]}   |
      | 14 | Traverse       | 12           |                                                                                                                         |
      | 12 | Traverse       | 11           |                                                                                                                         |
      | 11 | Argument       |              |                                                                                                                         |

  Scenario: Remove AppendVertices below inner join
    When profiling query:
      """
      MATCH (me:player)-[:like]->(both)
      WHERE id(me) == "Tony Parker"
      MATCH (he:player)-[:like]->(both)
      WHERE id(he) == "Tim Duncan"
      RETURN *
      """
    Then the result should be, in order, with relax comparison:
      | me              | both              | he             |
      | ("Tony Parker") | ("Manu Ginobili") | ("Tim Duncan") |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                             |
      | 13 | HashInnerJoin  | 6,12         | {"hashKeys": ["_joinkey($-.both)"], "probeKeys": ["$-.both"]}             |
      | 6  | Project        | 5            |                                                                           |
      | 5  | AppendVertices | 15           |                                                                           |
      | 15 | Traverse       | 2            |                                                                           |
      | 2  | Dedup          | 1            |                                                                           |
      | 1  | PassThrough    | 3            |                                                                           |
      | 3  | Start          |              |                                                                           |
      | 12 | Project        | 16           | {"columns": ["$-.he AS he", "none_direct_dst($-.__VAR_1,$-.he) AS both"]} |
      | 16 | Traverse       | 8            |                                                                           |
      | 8  | Dedup          | 7            |                                                                           |
      | 7  | PassThrough    | 9            |                                                                           |
      | 9  | Start          |              |                                                                           |

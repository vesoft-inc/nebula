# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Optimize left join predicate

  Background:
    Given a graph with space named "nba"

  Scenario: optimize left join predicate
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
        ORDER BY numFriends DESC
        LIMIT 20
      """
    Then the result should be, in any order, with relax comparison:
      | teamId          | teamName        | numFriends |
      | "Clippers"      | "Clippers"      | 0          |
      | "Bulls"         | "Bulls"         | 0          |
      | "Spurs"         | "Spurs"         | 0          |
      | "Thunders"      | "Thunders"      | 0          |
      | "Hornets"       | "Hornets"       | 0          |
      | "Warriors"      | "Warriors"      | 0          |
      | "Hawks"         | "Hawks"         | 0          |
      | "Kings"         | "Kings"         | 0          |
      | "Magic"         | "Magic"         | 0          |
      | "Trail Blazers" | "Trail Blazers" | 0          |
      | "Lakers"        | "Lakers"        | 0          |
      | "Grizzlies"     | "Grizzlies"     | 0          |
      | "Suns"          | "Suns"          | 0          |
      | "Rockets"       | "Rockets"       | 0          |
      | "Cavaliers"     | "Cavaliers"     | 0          |
      | "Raptors"       | "Raptors"       | 0          |
      | "Celtics"       | "Celtics"       | 0          |
      | "76ers"         | "76ers"         | 0          |
      | "Heat"          | "Heat"          | 0          |
      | "Jazz"          | "Jazz"          | 0          |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 21 | TopN           | 18           |                |               |
      | 18 | Project        | 17           |                |               |
      | 17 | Aggregate      | 16           |                |               |
      | 16 | BiLeftJoin     | 10,15        |                |               |
      | 10 | Dedup          | 28           |                |               |
      | 28 | Project        | 22           |                |               |
      | 22 | Filter         | 26           |                |               |
      | 26 | AppendVertices | 25           |                |               |
      | 25 | Traverse       | 24           |                |               |
      | 24 | Traverse       | 2            |                |               |
      | 2  | Dedup          | 1            |                |               |
      | 1  | PassThrough    | 3            |                |               |
      | 3  | Start          |              |                |               |
      | 15 | Project        | 14           |                |               |
      | 14 | Traverse       | 12           |                |               |
      | 12 | Traverse       | 11           |                |               |
      | 11 | Argument       |              |                |               |

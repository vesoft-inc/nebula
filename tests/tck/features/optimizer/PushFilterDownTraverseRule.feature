# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down Traverse rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down Traverse
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE e.likeness > 99 RETURN e.likeness, v2.player.age
      """
    Then the result should be, in any order:
      | e.likeness | v2.player.age |
      | 100        | 31            |
      | 100        | 43            |
    # The filter `e.likeness>99` is first pushed down to the `eFilter_` of `Traverese` by rule `PushFilterDownTraverse`,
    # and then pushed down to the `filter_` of `Traverse` by rule `PushEFilterDown`.
    And the execution plan should be:
      | id | name           | dependencies | operator info                    |
      | 6  | Project        | 5            |                                  |
      | 5  | AppendVertices | 10           |                                  |
      | 10 | Traverse       | 2            | {"filter": "(like.likeness>99)"} |
      | 2  | Dedup          | 9            |                                  |
      | 9  | IndexScan      | 3            |                                  |
      | 3  | Start          |              |                                  |
    When profiling query:
      """
      MATCH (person:player)-[:like*1..2]-(friend:player)-[served:serve]->(friendTeam:team)
      WHERE id(person) == "Tony Parker" AND id(friend) != "Tony Parker" AND served.start_year > 2010
      WITH DISTINCT friend, friendTeam
      OPTIONAL MATCH (friend)<-[:like]-(friend2:player)<-[:like]-(friendTeam)
      WITH friendTeam, count(friend2) AS numFriends
      RETURN
        friendTeam.team.name AS teamName,
        numFriends
        ORDER BY teamName DESC
        LIMIT 8
      """
    Then the result should be, in any order, with relax comparison:
      | teamName        | numFriends |
      | "Warriors"      | 0          |
      | "Trail Blazers" | 0          |
      | "Spurs"         | 0          |
      | "Rockets"       | 0          |
      | "Raptors"       | 0          |
      | "Pistons"       | 0          |
      | "Lakers"        | 0          |
      | "Kings"         | 0          |
    # The filter `served.start_year` is first pushed down to the `eFilter_` of `Traverese` by rule `PushFilterDownTraverse`,
    # and then pushed down to the `filter_` of `Traverse` by rule `PushEFilterDown`.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                         |
      | 21 | TopN           | 18           |                |                                       |
      | 18 | Project        | 17           |                |                                       |
      | 17 | Aggregate      | 16           |                |                                       |
      | 16 | HashLeftJoin   | 10,15        |                |                                       |
      | 10 | Dedup          | 28           |                |                                       |
      | 28 | Project        | 22           |                |                                       |
      | 22 | Filter         | 26           |                |                                       |
      | 26 | AppendVertices | 25           |                |                                       |
      | 25 | Traverse       | 24           |                | {"filter": "(serve.start_year>2010)"} |
      | 24 | Traverse       | 2            |                |                                       |
      | 2  | Dedup          | 1            |                |                                       |
      | 1  | PassThrough    | 3            |                |                                       |
      | 3  | Start          |              |                |                                       |
      | 15 | Project        | 14           |                |                                       |
      | 14 | Traverse       | 12           |                |                                       |
      | 12 | Traverse       | 11           |                |                                       |
      | 11 | Argument       |              |                |                                       |

  Scenario: push filter down Traverse with complex filter
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE v.player.age != 35 and (e.likeness + 100) != 199
      RETURN e.likeness, v2.player.age as age
      ORDER BY age
      LIMIT 3
      """
    Then the result should be, in any order:
      | e.likeness | age |
      | 90         | 20  |
      | 80         | 22  |
      | 90         | 23  |
    # The filter `e.likeness+100!=99` is first pushed down to the `eFilter_` of `Traverese` by rule `PushFilterDownTraverse`,
    # and then pushed down to the `filter_` of `Traverse` by rule `PushEFilterDown`.
    And the execution plan should be:
      | id | name           | dependencies | operator info                            |
      | 11 | TopN           | 10           |                                          |
      | 10 | Project        | 9            |                                          |
      | 9  | Filter         | 4            | {"condition": "($-.v.player.age!=35)" }  |
      | 4  | AppendVertices | 12           |                                          |
      | 12 | Traverse       | 8            | {"filter": "((like.likeness+100)!=199)"} |
      | 8  | IndexScan      | 2            |                                          |
      | 2  | Start          |              |                                          |

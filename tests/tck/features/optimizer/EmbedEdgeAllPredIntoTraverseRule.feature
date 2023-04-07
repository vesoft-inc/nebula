# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
@czp
Feature: Embed edge all predicate into Traverse

  Background:
    Given a graph with space named "nba"

  Scenario: Embed edge all predicate into Traverse
    When profiling query:
      """
      MATCH (v:player)-[e:like*1]->(n)
      WHERE all(i in e where i.likeness>90)
      RETURN [i in e | i.likeness] AS likeness, n.player.age AS nage
      """
    Then the result should be, in any order:
      | likeness | nage |
      | [99]     | 33   |
      | [99]     | 31   |
      | [99]     | 29   |
      | [99]     | 30   |
      | [99]     | 25   |
      | [99]     | 34   |
      | [99]     | 41   |
      | [99]     | 32   |
      | [99]     | 30   |
      | [99]     | 42   |
      | [99]     | 36   |
      | [95]     | 41   |
      | [95]     | 42   |
      | [100]    | 31   |
      | [95]     | 30   |
      | [95]     | 41   |
      | [95]     | 36   |
      | [100]    | 43   |
      | [99]     | 34   |
      | [99]     | 38   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                    |
      | 7  | Project        | 11           |                |                                  |
      | 11 | AppendVertices | 13           |                |                                  |
      | 13 | Traverse       | 1            |                | {"filter": "(like.likeness>90)"} |
      | 1  | IndexScan      | 2            |                |                                  |
      | 2  | Start          |              |                |                                  |
    When profiling query:
      """
      MATCH (v:player)-[e:like*1]->(n)
      WHERE all(i in e where i.likeness>90 or i.likeness<0)
      RETURN [i in e | i.likeness] AS likeness, n.player.age AS nage
      """
    Then the result should be, in any order:
      | likeness | nage |
      | [99]     | 33   |
      | [99]     | 31   |
      | [99]     | 29   |
      | [99]     | 30   |
      | [99]     | 25   |
      | [99]     | 34   |
      | [99]     | 41   |
      | [99]     | 32   |
      | [99]     | 30   |
      | [99]     | 42   |
      | [99]     | 36   |
      | [95]     | 41   |
      | [95]     | 42   |
      | [100]    | 31   |
      | [95]     | 30   |
      | [95]     | 41   |
      | [95]     | 36   |
      | [100]    | 43   |
      | [99]     | 34   |
      | [99]     | 38   |
      | [-1]     | 43   |
      | [-1]     | 33   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                                           |
      | 7  | Project        | 11           |                |                                                         |
      | 11 | AppendVertices | 13           |                |                                                         |
      | 13 | Traverse       | 1            |                | {"filter": "((like.likeness>90) OR (like.likeness<0))"} |
      | 1  | IndexScan      | 2            |                |                                                         |
      | 2  | Start          |              |                |                                                         |
    When profiling query:
      """
      MATCH (v:player)-[e:like*1]->(n)
      WHERE all(i in e where i.likeness>90 and v.player.name <> "x")
      RETURN [i in e | i.likeness] AS likeness, n.player.age AS nage
      """
    Then the result should be, in any order:
      | likeness | nage |
      | [99]     | 33   |
      | [99]     | 31   |
      | [99]     | 29   |
      | [99]     | 30   |
      | [99]     | 25   |
      | [99]     | 34   |
      | [99]     | 41   |
      | [99]     | 32   |
      | [99]     | 30   |
      | [99]     | 42   |
      | [99]     | 36   |
      | [95]     | 41   |
      | [95]     | 42   |
      | [100]    | 31   |
      | [95]     | 30   |
      | [95]     | 41   |
      | [95]     | 36   |
      | [100]    | 43   |
      | [99]     | 34   |
      | [99]     | 38   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                                                                                 |
      | 7  | Project        | 11           |                |                                                                                               |
      | 11 | Filter         | 11           |                | {"condition": "all(__VAR_0 IN $e WHERE (($__VAR_0.likeness>90) AND (v.player.name!=\"x\")))"} |
      | 11 | AppendVertices | 13           |                |                                                                                               |
      | 13 | Traverse       | 1            |                |                                                                                               |
      | 1  | IndexScan      | 2            |                |                                                                                               |
      | 2  | Start          |              |                |                                                                                               |
    When profiling query:
      """
      MATCH (v:player)-[e:like*2]->(n)
      WHERE all(i in e where i.likeness>90)
      RETURN [i in e | i.likeness] AS likeness, n.player.age AS nage
      """
    Then the result should be, in any order:
      | likeness  | nage |
      | [99, 100] | 43   |
      | [99, 95]  | 41   |
      | [99, 95]  | 36   |
      | [99, 95]  | 41   |
      | [99, 95]  | 42   |
      | [95, 95]  | 41   |
      | [95, 95]  | 36   |
      | [95, 95]  | 41   |
      | [95, 95]  | 42   |
      | [99, 99]  | 38   |
      | [99, 99]  | 34   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                    |
      | 7  | Project        | 11           |                |                                  |
      | 11 | AppendVertices | 13           |                |                                  |
      | 13 | Traverse       | 1            |                | {"filter": "(like.likeness>90)"} |
      | 1  | IndexScan      | 2            |                |                                  |
      | 2  | Start          |              |                |                                  |
    When profiling query:
      """
      MATCH (v:player)-[e:like*2..4]->(n)
      WHERE all(i in e where i.likeness>90)
      RETURN [i in e | i.likeness] AS likeness, n.player.age AS nage
      """
    Then the result should be, in any order:
      | likeness         | nage |
      | [99, 100]        | 43   |
      | [99, 95]         | 41   |
      | [99, 95]         | 36   |
      | [99, 95]         | 41   |
      | [99, 95]         | 42   |
      | [99, 95, 95]     | 41   |
      | [99, 95, 95]     | 42   |
      | [99, 95, 95]     | 41   |
      | [99, 95, 95]     | 36   |
      | [99, 95, 95, 95] | 41   |
      | [99, 95, 95, 95] | 41   |
      | [95, 95]         | 41   |
      | [95, 95]         | 36   |
      | [95, 95, 95]     | 41   |
      | [95, 95]         | 41   |
      | [95, 95]         | 42   |
      | [95, 95, 95]     | 41   |
      | [99, 99]         | 38   |
      | [99, 99]         | 34   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                    |
      | 7  | Project        | 11           |                |                                  |
      | 11 | AppendVertices | 13           |                |                                  |
      | 13 | Traverse       | 1            |                | {"filter": "(like.likeness>90)"} |
      | 1  | IndexScan      | 2            |                |                                  |
      | 2  | Start          |              |                |                                  |
    When profiling query:
      """
      MATCH (v:player)-[e:like*0..5]->(n)
      WHERE all(i in e where i.likeness>90) AND size(e)>0 AND (n.player.age>0) == true
      RETURN [i in e | i.likeness] AS likeness, n.player.age AS nage
      """
    Then the result should be, in any order:
      | likeness         | nage |
      | [99, 99]         | 34   |
      | [99]             | 38   |
      | [99, 99]         | 38   |
      | [99]             | 34   |
      | [100]            | 43   |
      | [95, 95, 95]     | 41   |
      | [95, 95]         | 42   |
      | [95, 95]         | 41   |
      | [95]             | 36   |
      | [95]             | 41   |
      | [95]             | 30   |
      | [100]            | 31   |
      | [95, 95, 95]     | 41   |
      | [95, 95]         | 36   |
      | [95, 95]         | 41   |
      | [95]             | 42   |
      | [95]             | 41   |
      | [99, 95, 95, 95] | 41   |
      | [99, 95, 95, 95] | 41   |
      | [99, 95, 95]     | 36   |
      | [99, 95, 95]     | 41   |
      | [99, 95, 95]     | 42   |
      | [99, 95, 95]     | 41   |
      | [99, 95]         | 42   |
      | [99, 95]         | 41   |
      | [99, 95]         | 36   |
      | [99, 95]         | 41   |
      | [99, 100]        | 43   |
      | [99]             | 36   |
      | [99]             | 42   |
      | [99]             | 30   |
      | [99]             | 32   |
      | [99]             | 41   |
      | [99]             | 34   |
      | [99]             | 25   |
      | [99]             | 30   |
      | [99]             | 29   |
      | [99]             | 31   |
      | [99]             | 33   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                             |
      | 7  | Project        | 15           |                |                                           |
      | 15 | Filter         | 11           |                | {"condition": "((n.player.age>0)==true)"} |
      | 11 | AppendVertices | 14           |                |                                           |
      | 14 | Filter         | 13           |                | {"condition": "(size($e)>0)"}             |
      | 13 | Traverse       | 1            |                | {"edge filter": "(*.likeness>90)"}        |
      | 1  | IndexScan      | 2            |                |                                           |
      | 2  | Start          |              |                |                                           |
    When profiling query:
      """
      MATCH (v:player)-[e:like*1..5]->(n)
      WHERE all(i in e where i.likeness>90) AND size(e)>0 AND (n.player.age>0) == true
      RETURN [i in e | i.likeness] AS likeness, n.player.age AS nage
      """
    Then the result should be, in any order:
      | likeness         | nage |
      | [99, 99]         | 34   |
      | [99]             | 38   |
      | [99, 99]         | 38   |
      | [99]             | 34   |
      | [100]            | 43   |
      | [95, 95, 95]     | 41   |
      | [95, 95]         | 42   |
      | [95, 95]         | 41   |
      | [95]             | 36   |
      | [95]             | 41   |
      | [95]             | 30   |
      | [100]            | 31   |
      | [95, 95, 95]     | 41   |
      | [95, 95]         | 36   |
      | [95, 95]         | 41   |
      | [95]             | 42   |
      | [95]             | 41   |
      | [99, 95, 95, 95] | 41   |
      | [99, 95, 95, 95] | 41   |
      | [99, 95, 95]     | 36   |
      | [99, 95, 95]     | 41   |
      | [99, 95, 95]     | 42   |
      | [99, 95, 95]     | 41   |
      | [99, 95]         | 42   |
      | [99, 95]         | 41   |
      | [99, 95]         | 36   |
      | [99, 95]         | 41   |
      | [99, 100]        | 43   |
      | [99]             | 36   |
      | [99]             | 42   |
      | [99]             | 30   |
      | [99]             | 32   |
      | [99]             | 41   |
      | [99]             | 34   |
      | [99]             | 25   |
      | [99]             | 30   |
      | [99]             | 29   |
      | [99]             | 31   |
      | [99]             | 33   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                             |
      | 7  | Project        | 15           |                |                                           |
      | 15 | Filter         | 11           |                | {"condition": "((n.player.age>0)==true)"} |
      | 11 | AppendVertices | 14           |                |                                           |
      | 14 | Filter         | 13           |                | {"condition": "(size($e)>0)"}             |
      | 13 | Traverse       | 1            |                | {"filter": "(like.likeness>90)"}          |
      | 1  | IndexScan      | 2            |                |                                           |
      | 2  | Start          |              |                |                                           |
    When profiling query:
      """
      MATCH (v:player)-[e:like*0..5]->(n)
      WHERE all(i in e where i.likeness>90) AND not all(i in e where i.likeness > 89)
      RETURN [i in e | i.likeness] AS likeness, n.player.age AS nage
      """
    Then the result should be, in any order:
      | likeness | nage |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                                                                                                                                                                       |
      | 7  | Project        | 11           |                |                                                                                                                                                                                     |
      | 11 | AppendVertices | 14           |                |                                                                                                                                                                                     |
      | 14 | Filter         | 13           |                | {"condition": "(!(all(__VAR_1 IN $e WHERE ($__VAR_1.likeness>89))) AND !(all(__VAR_1 IN $e WHERE ($__VAR_1.likeness>89))) AND !(all(__VAR_1 IN $e WHERE ($__VAR_1.likeness>89))))"} |
      | 13 | Traverse       | 1            |                | {"edge filter": "(*.likeness>90)"}                                                                                                                                                  |
      | 1  | IndexScan      | 2            |                |                                                                                                                                                                                     |
      | 2  | Start          |              |                |                                                                                                                                                                                     |
    When profiling query:
      """
      MATCH (person:player)-[e1:like*1..2]-(friend:player)
      WHERE id(person) == "Tony Parker" AND id(friend) != "Tony Parker" AND  all(i in e1 where i.likeness > 0)
      MATCH (friend)-[served:serve]->(friendTeam:team)
      WHERE served.start_year > 2010 AND all(i in e1 where i.likeness > 1)
      WITH DISTINCT friend, friendTeam
      OPTIONAL MATCH (friend)<-[e2:like*2..4]-(friend2:player)<-[:like]-(friendTeam)
      WITH friendTeam, count(friend2) AS numFriends, e2
      WHERE e2 IS NULL OR all(i in e2 where i IS NULL)
      RETURN
        friendTeam.team.name AS teamName,
        numFriends,
        [i in e2 | i.likeness] AS likeness
        ORDER BY teamName DESC
        LIMIT 8
      """
    Then the result should be, in any order, with relax comparison:
      | teamName        | numFriends | likeness |
      | "Warriors"      | 0          | NULL     |
      | "Trail Blazers" | 0          | NULL     |
      | "Spurs"         | 0          | NULL     |
      | "Rockets"       | 0          | NULL     |
      | "Raptors"       | 0          | NULL     |
      | "Pistons"       | 0          | NULL     |
      | "Lakers"        | 0          | NULL     |
      | "Kings"         | 0          | NULL     |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                                                                |
      | 28 | TopN           | 24           |                |                                                                              |
      | 24 | Project        | 30           |                |                                                                              |
      | 30 | Aggregate      | 29           |                |                                                                              |
      | 29 | Filter         | 44           |                | {"condition": "($e2 IS NULL OR all(__VAR_2 IN $e2 WHERE $__VAR_2 IS NULL))"} |
      | 44 | HashLeftJoin   | 15,43        |                |                                                                              |
      | 15 | Dedup          | 14           |                |                                                                              |
      | 14 | Project        | 35           |                |                                                                              |
      | 35 | HashInnerJoin  | 53,50        |                |                                                                              |
      | 53 | Filter         | 52           |                | {"condition": "(id($friend)!=\"Tony Parker\")"}                              |
      | 52 | AppendVertices | 59           |                |                                                                              |
      | 59 | Filter         | 60           |                | {"condition": "(id($person)==\"Tony Parker\")"}                              |
      | 60 | Traverse       | 2            |                | {"filter": "((like.likeness>0) AND (like.likeness>1))"}                      |
      | 2  | Dedup          | 1            |                |                                                                              |
      | 1  | PassThrough    | 3            |                |                                                                              |
      | 3  | Start          |              |                |                                                                              |
      | 50 | Project        | 55           |                |                                                                              |
      | 55 | AppendVertices | 61           |                |                                                                              |
      | 61 | Traverse       | 8            |                |                                                                              |
      | 8  | Argument       |              |                |                                                                              |
      | 43 | Project        | 39           |                |                                                                              |
      | 39 | Traverse       | 17           |                |                                                                              |
      | 17 | Traverse       | 16           |                |                                                                              |
      | 16 | Argument       |              |                |                                                                              |

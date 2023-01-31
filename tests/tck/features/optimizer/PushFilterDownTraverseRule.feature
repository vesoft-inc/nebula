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
      MATCH (v:player)-[e:like]->(v2) WHERE rank(e) == 0 RETURN COUNT(*)
      """
    Then the result should be, in any order:
      | COUNT(*) |
      | 81       |
    And the execution plan should be:
      | id | name           | dependencies | operator info                 |
      | 7  | Aggregate      | 6            |                               |
      | 6  | Project        | 5            |                               |
      | 5  | AppendVertices | 10           |                               |
      | 10 | Traverse       | 2            | {"filter": "(like._rank==0)"} |
      | 2  | Dedup          | 9            |                               |
      | 9  | IndexScan      | 3            |                               |
      | 3  | Start          |              |                               |
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE none_direct_dst(e) IN ["Tony Parker", "Tim Duncan"] RETURN e.likeness, v2.player.age
      """
    Then the result should be, in any order:
      | e.likeness | v2.player.age |
      | 80         | 36            |
      | 99         | 36            |
      | 75         | 36            |
      | 50         | 36            |
      | 95         | 36            |
      | 80         | 42            |
      | 80         | 42            |
      | 70         | 42            |
      | 99         | 42            |
      | 75         | 42            |
      | 90         | 42            |
      | 55         | 42            |
      | 80         | 42            |
      | 80         | 42            |
      | 95         | 42            |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                 |
      | 6  | Project        | 5            |                                                               |
      | 5  | AppendVertices | 10           |                                                               |
      | 10 | Traverse       | 9            | {"filter": "(like._dst IN [\"Tony Parker\",\"Tim Duncan\"])"} |
      | 9  | IndexScan      | 3            |                                                               |
      | 3  | Start          |              |                                                               |
    # The following two match statements is equivalent, so the minus of them should be empty.
    When executing query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE none_direct_dst(e) IN ["Tony Parker", "Tim Duncan"] RETURN *
      MINUS
      MATCH (v:player)-[e:like]->(v2) WHERE id(v2) IN ["Tony Parker", "Tim Duncan"] RETURN *
      """
    Then the result should be, in any order:
      | v | e | v2 |
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE none_direct_src(e) IN ["Tony Parker", "Tim Duncan"] RETURN e.likeness, v2.player.age
      """
    Then the result should be, in any order:
      | e.likeness | v2.player.age |
      | 90         | 33            |
      | 95         | 41            |
      | 95         | 42            |
      | 95         | 41            |
      | 95         | 36            |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                 |
      | 6  | Project        | 5            |                                                               |
      | 5  | AppendVertices | 10           |                                                               |
      | 10 | Traverse       | 9            | {"filter": "(like._src IN [\"Tony Parker\",\"Tim Duncan\"])"} |
      | 9  | IndexScan      | 3            |                                                               |
      | 3  | Start          |              |                                                               |
    # The following two match statements is equivalent, so the minus of them should be empty.
    When executing query:
      """
      MATCH (v:player)-[e:like]->(v2) WHERE none_direct_src(e) IN ["Tony Parker", "Tim Duncan"] RETURN *
      MINUS
      MATCH (v:player)-[e:like]->(v2) WHERE id(v) IN ["Tony Parker", "Tim Duncan"] RETURN *
      """
    Then the result should be, in any order:
      | v | e | v2 |
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
    When profiling query:
      """
      MATCH (v)-[e1:like]->(v1) WHERE id(v) == "Tony Parker"
      WITH DISTINCT v1, e1.degree AS strength
      ORDER BY strength DESC LIMIT 20
      MATCH (v1)<-[e2:like]-(v2)
      WITH v1, e2, v2 WHERE none_direct_dst(e2) IN ["Yao Ming", "Tim Duncan"]
      RETURN id(v2) AS candidate, count(*) AS cnt;
      """
    Then the result should be, in any order, with relax comparison:
      | candidate    | cnt |
      | "Tim Duncan" | 1   |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info                                              |
      | 18 | Aggregate      | 22           |                |                                                            |
      | 22 | Project        | 25           |                |                                                            |
      | 25 | HashInnerJoin  | 20,27        |                |                                                            |
      | 20 | TopN           | 8            |                |                                                            |
      | 8  | Dedup          | 19           |                |                                                            |
      | 19 | Project        | 5            |                |                                                            |
      | 5  | AppendVertices | 4            |                |                                                            |
      | 4  | Traverse       | 2            |                |                                                            |
      | 2  | Dedup          | 1            |                |                                                            |
      | 1  | PassThrough    | 3            |                |                                                            |
      | 3  | Start          |              |                |                                                            |
      | 27 | Project        | 28           |                |                                                            |
      | 28 | AppendVertices | 30           |                |                                                            |
      | 30 | Traverse       | 11           |                | {"filter": "(like._dst IN [\"Yao Ming\",\"Tim Duncan\"])"} |
      | 11 | Argument       |              |                |                                                            |

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
    When profiling query:
      """
      MATCH (v:player)-[e:like]->(v2)
        WHERE v.player.age != 35 and (e.likeness + 100) != 199 and none_direct_dst(e) in ["Tony Parker", "Tim Duncan", "Yao Ming"]
      RETURN e.likeness, v2.player.age as age
      ORDER BY age
      LIMIT 3
      """
    Then the result should be, in any order:
      | e.likeness | age |
      | 80         | 36  |
      | 75         | 36  |
      | 50         | 36  |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                                                             |
      | 11 | TopN           | 10           |                                                                                                                                           |
      | 10 | Project        | 9            |                                                                                                                                           |
      | 9  | Filter         | 4            | {"condition": "($-.v.player.age!=35)" }                                                                                                   |
      | 4  | AppendVertices | 12           |                                                                                                                                           |
      | 12 | Traverse       | 8            | {"filter": "(((like.likeness+100)!=199) AND ((like._dst==\"Tony Parker\") OR (like._dst==\"Tim Duncan\") OR (like._dst==\"Yao Ming\")))"} |
      | 8  | IndexScan      | 2            |                                                                                                                                           |
      | 2  | Start          |              |                                                                                                                                           |

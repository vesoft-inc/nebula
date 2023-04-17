# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down Aggregate rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down Aggregate
    When profiling query:
      """
      MATCH (v:player)
      WITH
        v.player.age+1 AS age,
        COUNT(v.player.age) as count
      WHERE age<30
      RETURN age, count
      ORDER BY age
      """
    Then the result should be, in any order:
      | age | count |
      | -3  | 1     |
      | -2  | 1     |
      | -1  | 1     |
      | 0   | 1     |
      | 1   | 1     |
      | 21  | 1     |
      | 23  | 1     |
      | 24  | 1     |
      | 25  | 1     |
      | 26  | 2     |
      | 27  | 1     |
      | 28  | 1     |
      | 29  | 3     |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 8  | Sort           | 13           |               |
      | 13 | Aggregate      | 12           |               |
      | 12 | AppendVertices | 1            |               |
      | 1  | IndexScan      | 2            |               |
      | 2  | Start          |              |               |

  Scenario: push filter down aggregate and project
    When profiling query:
      """
      match (n0)-[e:like]->(n1:player{name: "Tim Duncan"})
      where id(n1) == "Tim Duncan"
      with min(87) as n0, n1.player.age as age
      where age > 10
      return *
      """
    Then the result should be, in any order:
      | n0 | age |
      | 87 | 42  |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 15 | Aggregate      | 13           |               |
      | 13 | Project        | 12           |               |
      | 12 | Filter         | 5            |               |
      | 5  | AppendVertices | 14           |               |
      | 14 | Traverse       | 2            |               |
      | 2  | Dedup          | 1            |               |
      | 1  | PassThrough    | 3            |               |
      | 3  | Start          |              |               |

  Scenario: push multiple filters down aggregate
    When executing query:
      """
      match (v0:player)-[e0:serve]->(v1)<-[e1:serve]-(v2:player)
      where id(v0) in ["Tim Duncan", "Aron Baynes", "Ben Simmons"]
      with v0, avg(v0.player.age + e0.start_year + v2.player.age + 1000) as pi0,
      endNode(e0) as pi1
      with (v0.player.age + 1000 ) / pi1.player.age as pi0, pi1, pi1.player.age + 50 as pi2,
      v0, pi1.player.name as pi3, avg(v0.player.age * 100 + pi0) as pi4
      where v0.player.name == "Ben Simmons"
      with (v0.player.age + 1000 ) / pi1.player.age as pi0, pi1, pi1.player.age + 50 as pi2,
      v0, pi1.player.name as pi3, avg(v0.player.age * 100 + pi0) as pi4
      where v0.player.name == "Ben Simmons"
      with v0.player.name as pi0 where pi0 == pi0
      return *
      """
    Then the execution should be successful

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test push filter down project

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down project
    When profiling query:
      """
      MATCH (n0)-[:like]->(n1)
      WHERE id(n0) IN ['Tim Duncan']
      WITH n1.player.age AS a0
      WHERE (a0 - (a0 + ((a0 % a0) + (a0 + a0)))) <= a0
      RETURN count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 2        |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                                                           |
      | 9  | Aggregate      | 12           |                                                                                                         |
      | 12 | Project        | 5            |                                                                                                         |
      | 5  | AppendVertices | 4            | {"filter": "((player.age-(player.age+((player.age%player.age)+(player.age+player.age))))<=player.age)"} |
      | 4  | Traverse       | 2            |                                                                                                         |
      | 2  | Dedup          | 1            |                                                                                                         |
      | 1  | PassThrough    | 3            |                                                                                                         |
      | 3  | Start          |              |                                                                                                         |

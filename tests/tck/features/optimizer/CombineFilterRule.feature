# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: combine filters

  Background:
    Given a graph with space named "nba"

  Scenario: combine filters
    When profiling query:
      """
      MATCH (v:player)-[:like]->(n)
      WHERE v.player.age > 40 AND n.player.age > 42
      RETURN v, n
      """
    Then the result should be, in any order:
      | v                                                       | n                                                   |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})     | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"}) |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"}) | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"}) |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})     | ("Steve Nash" :player{age: 45, name: "Steve Nash"}) |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 10 | Project        | 9            |               |
      | 9  | Filter         | 3            |               |
      | 3  | AppendVertices | 2            |               |
      | 2  | Traverse       | 7            |               |
      | 7  | IndexScan      | 0            |               |
      | 0  | Start          |              |               |

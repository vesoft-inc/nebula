# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: combine filters

  Background:
    Given a graph with space named "nba"

  Scenario: combine filters
    When profiling query:
      """
      MATCH (v:player)-[:like]->(n)
      WHERE v.age>40 AND n.age>42
      RETURN v, n
      """
    Then the result should be, in any order:
      | v                                                       | n                                                   |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})     | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"}) |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"}) | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"}) |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})     | ("Steve Nash" :player{age: 45, name: "Steve Nash"}) |
    And the execution plan should be:
      | id | name         | dependencies | operator info                                                                       |
      | 16 | Project      | 18           |                                                                                     |
      | 18 | Filter       | 13           | {"condition": "(($v.age>40) AND ($n.age>42) AND !(hasSameEdgeInPath($-.__COL_0)))"} |
      | 13 | Project      | 12           |                                                                                     |
      | 12 | InnerJoin    | 11           |                                                                                     |
      | 11 | Project      | 20           |                                                                                     |
      | 20 | GetVertices  | 7            |                                                                                     |
      | 7  | Filter       | 6            |                                                                                     |
      | 6  | Project      | 5            |                                                                                     |
      | 5  | Filter       | 22           |                                                                                     |
      | 22 | GetNeighbors | 17           |                                                                                     |
      | 17 | IndexScan    | 0            |                                                                                     |
      | 0  | Start        |              |                                                                                     |

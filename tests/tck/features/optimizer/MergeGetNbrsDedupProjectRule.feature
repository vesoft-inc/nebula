# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: merge get neighbors, dedup and project rule

  Background:
    Given a graph with space named "nba"

  Scenario: apply get nbrs, dedup and project merge opt rule
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like*0..1]->(v2)
      RETURN v2.name AS Name
      """
    Then the result should be, in any order:
      | Name            |
      | "Manu Ginobili" |
      | "Tony Parker"   |
      | "Tim Duncan"    |
    And the execution plan should be:
      | name               | dependencies | operator info |
      | Project            | 1            |               |
      | Filter             | 2            |               |
      | Project            | 3            |               |
      | DataJoin           | 4            |               |
      | Project            | 5            |               |
      | GetVertices        | 6            | dedup: true   |
      | Filter             | 7            |               |
      | UnionAllVersionVar | 8            |               |
      | Loop               | 15           | loopBody: 9   |
      | Filter             | 10           |               |
      | Project            | 11           |               |
      | DataJoin           | 12           |               |
      | Project            | 13           |               |
      | GetNeighbors       | 14           | dedup: true   |
      | Start              |              |               |
      | Project            | 16           |               |
      | Filter             | 17           |               |
      | GetVertices        | 18           | dedup: true   |
      | IndexScan          | 19           |               |
      | Start              |              |               |

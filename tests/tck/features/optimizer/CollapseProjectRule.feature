# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Collapse Project Rule

  Background:
    Given a graph with space named "nba"

  Scenario: Collapse Project
    When profiling query:
      """
      MATCH (v:player)
      WHERE v.age > 38
      WITH v, v.age AS age, v.age/10 AS iage, v.age%10 AS mage,v.name AS name
      RETURN iage
      """
    Then the result should be, in any order:
      | iage |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 4    |
      | 3    |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 16 | Project     | 14           |               |
      | 14 | Filter      | 7            |               |
      | 7  | Project     | 6            |               |
      | 6  | Project     | 5            |               |
      | 5  | Filter      | 18           |               |
      | 18 | GetVertices | 12           |               |
      | 12 | IndexScan   | 0            |               |
      | 0  | Start       |              |               |
    When profiling query:
      """
      LOOKUP ON player
      WHERE player.name=='Tim Duncan'
      | YIELD $-.VertexID AS vid
      """
    Then the result should be, in any order:
      | vid          |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name               | dependencies | operator info |
      | 6  | Project            | 5            |               |
      | 5  | TagIndexPrefixScan | 0            |               |
      | 0  | Start              |              |               |

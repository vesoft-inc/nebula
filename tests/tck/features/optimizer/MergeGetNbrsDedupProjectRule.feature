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
      | id | name               | dependencies | operator info                                       |
      | 0  | Project            | 1            |                                                     |
      | 1  | Filter             | 2            |                                                     |
      | 2  | Project            | 3            |                                                     |
      | 3  | InnerJoin          | 4            |                                                     |
      | 4  | Project            | 5            |                                                     |
      | 5  | GetVertices        | 6            | {"dedup": "true"}                                   |
      | 6  | Filter             | 7            |                                                     |
      | 7  | UnionAllVersionVar | 8            |                                                     |
      | 8  | Loop               | 15           | {"loopBody": "9"}                                   |
      | 9  | Filter             | 10           |                                                     |
      | 10 | Project            | 11           |                                                     |
      | 11 | InnerJoin          | 12           | {"inputVar": {"rightVar":{"__Project_11":"0"}}}     |
      | 12 | Project            | 13           |                                                     |
      | 13 | GetNeighbors       | 14           | {"dedup": "true"}                                   |
      | 14 | Start              |              |                                                     |
      | 15 | Project            | 16           |                                                     |
      | 16 | Filter             | 17           |                                                     |
      | 17 | GetVertices        | 18           | {"dedup": "true"}                                   |
      | 18 | IndexScan          | 19           | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 19 | Start              |              |                                                     |

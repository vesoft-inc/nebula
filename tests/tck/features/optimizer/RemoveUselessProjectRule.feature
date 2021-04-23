# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Remove Useless Project Rule

  Background:
    Given a graph with space named "nba"

  Scenario: Remove useless project
    When profiling query:
      """
      MATCH (v:player)
      WITH
        v.age+1 AS age,
        count(v.age) AS count
      RETURN age, count
      ORDER BY age, count
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
      | 30  | 4     |
      | 31  | 4     |
      | 32  | 3     |
      | 33  | 3     |
      | 34  | 4     |
      | 35  | 4     |
      | 37  | 3     |
      | 38  | 1     |
      | 39  | 3     |
      | 40  | 1     |
      | 41  | 2     |
      | 42  | 1     |
      | 43  | 2     |
      | 44  | 1     |
      | 46  | 2     |
      | 47  | 1     |
      | 48  | 1     |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 12 | DataCollect | 11           |               |
      | 11 | Sort        | 14           |               |
      | 14 | Aggregate   | 8            |               |
      | 8  | Filter      | 7            |               |
      | 7  | Project     | 6            |               |
      | 6  | Project     | 5            |               |
      | 5  | Filter      | 16           |               |
      | 16 | GetVertices | 13           |               |
      | 13 | IndexScan   | 0            |               |
      | 0  | Start       |              |               |
    When profiling query:
      """
      MATCH p = (n:player{name:"Tony Parker"})
      RETURN n, p
      """
    Then the result should be, in any order:
      | n                                                     | p                                                       |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | <("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    And the execution plan should be:
      | id | name        | dependencies | operator info |
      | 11 | Filter      | 7            |               |
      | 7  | Project     | 6            |               |
      | 6  | Project     | 5            |               |
      | 5  | Filter      | 13           |               |
      | 13 | GetVertices | 10           |               |
      | 10 | IndexScan   | 0            |               |
      | 0  | Start       |              |               |

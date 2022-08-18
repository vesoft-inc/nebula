# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down Aggregate rule

  Background:
    Given a graph with space named "nba"

  # TODO(yee):
  @skip
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
      | id | name        | dependencies | operator info |
      | 12 | Sort        | 19           |               |
      | 19 | Aggregate   | 18           |               |
      | 18 | Filter      | 8            |               |
      | 8  | Filter      | 7            |               |
      | 7  | Project     | 6            |               |
      | 6  | Project     | 5            |               |
      | 5  | Filter      | 17           |               |
      | 17 | GetVertices | 14           |               |
      | 14 | IndexScan   | 0            |               |
      | 0  | Start       |              |               |

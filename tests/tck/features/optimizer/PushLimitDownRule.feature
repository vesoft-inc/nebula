# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Push Limit down rule

  Background:
    Given a graph with space named "nba"

  Scenario: push limit down to GetNeighbors
    When profiling query:
      """
      GO 1 STEPS FROM "James Harden" OVER like REVERSELY |
      Limit 2
      """
    Then the result should be, in any order:
      | like._dst         |
      | "Dejounte Murray" |
      | "Luka Doncic"     |
    And the execution plan should be:
      | id | name         | dependencies | operator info  |
      | 4  | DataCollect  | 5            |                |
      | 5  | Limit        | 6            |                |
      | 6  | Project      | 7            |                |
      | 7  | GetNeighbors | 0            | {"limit": "2"} |
      | 0  | Start        |              |                |

  Scenario: push limit down to GetNeighbors with offset
    When profiling query:
      """
      GO 1 STEPS FROM "Vince Carter" OVER serve
      YIELD serve.start_year as start_year |
      Limit 3, 4
      """
    Then the result should be, in any order:
      | start_year |
      | 2004       |
      | 2009       |
      | 2011       |
      | 1998       |
    And the execution plan should be:
      | id | name         | dependencies | operator info  |
      | 0  | DataCollect  | 1            |                |
      | 1  | Limit        | 2            |                |
      | 2  | Project      | 3            |                |
      | 3  | GetNeighbors | 4            | {"limit": "7"} |
      | 4  | Start        |              |                |

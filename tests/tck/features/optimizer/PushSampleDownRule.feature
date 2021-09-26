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
      GO 1 STEPS FROM "James Harden" OVER like REVERSELY LIMIT [2]
      """
    Then the result should be, in any order:
      | like._dst         |
      | "Dejounte Murray" |
      | "Luka Doncic"     |
    And the execution plan should be:
      | id | name         | dependencies | operator info  |
      | 5  | Project      | 6            |                |
      | 6  | Limit        | 7            |                |
      | 7  | GetNeighbors | 0            | {"limit": "2"} |
      | 0  | Start        |              |                |
    When profiling query:
      """
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY LIMIT [2, 2]
      """
    Then the result should be, in any order:
      | like._dst            |
      | "Kristaps Porzingis" |
    And the execution plan should be:
      | id | name         | dependencies | operator info                                          |
      | 9  | Project      | 12           |                                                        |
      | 12 | Limit        | 13           |                                                        |
      | 13 | GetNeighbors | 6            | {"limit": "2", "random": "false"}                      |
      | 6  | Loop         | 0            | {"loopBody": "5"}                                      |
      | 5  | Dedup        | 4            |                                                        |
      | 4  | Project      | 20           |                                                        |
      | 20 | Limit        | 21           |                                                        |
      | 21 | GetNeighbors | 1            | {"limit": "$__VAR_2[($__VAR_1-1)]", "random": "false"} |
      | 1  | Start        |              |                                                        |
      | 0  | Start        |              |                                                        |
    When profiling query:
      """
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY YIELD $^.player.name AS src, like.likeness AS likeness, $$.player.name AS dst LIMIT [2, 2]
      """
    Then the result should be, in any order:
      | src           | likeness | dst                  |
      | "Luka Doncic" | 90       | "Kristaps Porzingis" |
    And the execution plan should be:
      | id | name         | dependencies | profiling data | operator info                                          |
      | 13 | Project      | 12           |                |                                                        |
      | 12 | LeftJoin     | 11           |                |                                                        |
      | 11 | Project      | 10           |                |                                                        |
      | 10 | GetVertices  | 9            |                |                                                        |
      | 9  | Project      | 16           |                |                                                        |
      | 16 | Limit        | 17           |                |                                                        |
      | 17 | GetNeighbors | 6            |                | {"limit": "2", "random": "false"}                      |
      | 6  | Loop         | 0            |                | {"loopBody": "5"}                                      |
      | 5  | Dedup        | 4            |                |                                                        |
      | 4  | Project      | 24           |                |                                                        |
      | 24 | Limit        | 25           |                |                                                        |
      | 25 | GetNeighbors | 1            |                | {"limit": "$__VAR_2[($__VAR_1-1)]", "random": "false"} |
      | 1  | Start        |              |                |                                                        |
      | 0  | Start        |              |                |                                                        |

  Scenario: push sample down to GetNeighbors
    When profiling query:
      """
      GO 1 STEPS FROM "James Harden" OVER like REVERSELY SAMPLE [2]
      """
    Then the result should be, in any order:
      | like._dst |
      | /[\w\s]+/ |
      | /[\w\s]+/ |
    And the execution plan should be:
      | id | name         | dependencies | operator info                    |
      | 1  | Project      | 2            |                                  |
      | 2  | Sample       | 3            |                                  |
      | 3  | GetNeighbors | 4            | {"limit": "2", "random": "true"} |
      | 4  | Start        |              |                                  |
    When profiling query:
      """
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY SAMPLE [2, 2]
      """
    Then the result should be, in any order:
      | like._dst |
      | /[\w\s]+/ |
      | /[\w\s]+/ |
    And the execution plan should be:
      | id | name         | dependencies | operator info                                         |
      | 9  | Project      | 12           |                                                       |
      | 12 | Sample       | 13           |                                                       |
      | 13 | GetNeighbors | 6            | {"limit": "2", "random": "true"}                      |
      | 6  | Loop         | 0            | {"loopBody": "5"}                                     |
      | 5  | Dedup        | 4            |                                                       |
      | 4  | Project      | 20           |                                                       |
      | 20 | Sample       | 21           |                                                       |
      | 21 | GetNeighbors | 1            | {"limit": "$__VAR_2[($__VAR_1-1)]", "random": "true"} |
      | 1  | Start        |              |                                                       |
      | 0  | Start        |              |                                                       |
    When profiling query:
      """
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY YIELD $^.player.name AS src, like.likeness AS likeness, $$.player.name AS dst SAMPLE [2, 2]
      """
    Then the result should be, in any order:
      | src       | likeness | dst       |
      | /[\w\s]+/ | /\d\d/   | /[\w\s]+/ |
      | /[\w\s]+/ | /\d\d/   | /[\w\s]+/ |
    And the execution plan should be:
      | id | name         | dependencies | profiling data | operator info                                         |
      | 13 | Project      | 12           |                |                                                       |
      | 12 | LeftJoin     | 11           |                |                                                       |
      | 11 | Project      | 10           |                |                                                       |
      | 10 | GetVertices  | 9            |                |                                                       |
      | 9  | Project      | 16           |                |                                                       |
      | 16 | Sample       | 17           |                |                                                       |
      | 17 | GetNeighbors | 6            |                | {"limit": "2", "random": "true"}                      |
      | 6  | Loop         | 0            |                | {"loopBody": "5"}                                     |
      | 5  | Dedup        | 4            |                |                                                       |
      | 4  | Project      | 24           |                |                                                       |
      | 24 | Sample       | 25           |                |                                                       |
      | 25 | GetNeighbors | 1            |                | {"limit": "$__VAR_2[($__VAR_1-1)]", "random": "true"} |
      | 1  | Start        |              |                |                                                       |
      | 0  | Start        |              |                |                                                       |

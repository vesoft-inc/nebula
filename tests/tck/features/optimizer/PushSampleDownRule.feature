# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Limit down rule

  Background:
    Given a graph with space named "nba"

  Scenario: push limit down to GetNeighbors
    When profiling query:
      """
      GO 1 STEPS FROM "James Harden" OVER like REVERSELY YIELD like._dst LIMIT [2]
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
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY YIELD like._dst LIMIT [2, 2]
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
      | 14 | Project      | 13           |                |                                                        |
      | 13 | HashLeftJoin | 9, 12        |                |                                                        |
      | 9  | Project      | 15           |                |                                                        |
      | 15 | Limit        | 16           |                |                                                        |
      | 16 | GetNeighbors | 6            |                | {"limit": "2", "random": "false"}                      |
      | 6  | Loop         | 0            |                | {"loopBody": "5"}                                      |
      | 5  | Dedup        | 4            |                |                                                        |
      | 4  | Project      | 17           |                |                                                        |
      | 17 | Limit        | 18           |                |                                                        |
      | 18 | GetNeighbors | 1            |                | {"limit": "$__VAR_2[($__VAR_1-1)]", "random": "false"} |
      | 1  | Start        |              |                |                                                        |
      | 0  | Start        |              |                |                                                        |
      | 12 | Project      | 11           |                |                                                        |
      | 11 | GetVertices  | 10           |                |                                                        |
      | 10 | Argument     |              |                |                                                        |
    When profiling query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      GO 2 steps FROM $var.dst OVER like YIELD $var.dst AS dst1,like._dst AS dst2 LIMIT [2,3] | YIELD count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 3        |
    And the execution plan should be:
      | id | name         | dependencies | profiling data | operator info                                          |
      | 24 | Aggregate    | 23           |                |                                                        |
      | 23 | Project      | 22           |                |                                                        |
      | 22 | InnerJoin    | 21           |                |                                                        |
      | 21 | InnerJoin    | 20           |                |                                                        |
      | 20 | Project      | 26           |                |                                                        |
      | 26 | Limit        | 27           |                | { "count": "3" }                                       |
      | 27 | GetNeighbors | 17           |                | {"limit": "3", "random": "false"}                      |
      | 17 | Loop         | 11           |                | {"loopBody": "16"}                                     |
      | 16 | Dedup        | 15           |                |                                                        |
      | 15 | Project      | 14           |                |                                                        |
      | 14 | InnerJoin    | 13           |                |                                                        |
      | 13 | Dedup        | 12           |                |                                                        |
      | 12 | Project      | 9            |                |                                                        |
      | 9  | Dedup        | 8            |                |                                                        |
      | 8  | Project      | 34           |                |                                                        |
      | 34 | Limit        | 35           |                | {"count": "$__VAR_2[($__VAR_1-1)]"}                    |
      | 35 | GetNeighbors | 5            |                | {"limit": "$__VAR_2[($__VAR_1-1)]", "random": "false"} |
      | 5  | Start        |              |                |                                                        |
      | 11 | Dedup        | 10           |                |                                                        |
      | 10 | Project      | 4            |                |                                                        |
      | 4  | Dedup        | 3            |                |                                                        |
      | 3  | Project      | 2            |                |                                                        |
      | 2  | Project      | 1            |                |                                                        |
      | 1  | GetNeighbors | 0            |                |                                                        |
      | 0  | Start        |              |                |                                                        |

  Scenario: push sample down to GetNeighbors
    When profiling query:
      """
      GO 1 STEPS FROM "James Harden" OVER like REVERSELY YIELD like._dst SAMPLE [2]
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
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY YIELD like._dst SAMPLE [2, 2]
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
      | 14 | Project      | 13           |                |                                                       |
      | 13 | HashLeftJoin | 9, 12        |                |                                                       |
      | 9  | Project      | 17           |                |                                                       |
      | 17 | Sample       | 18           |                |                                                       |
      | 18 | GetNeighbors | 6            |                | {"limit": "2", "random": "true"}                      |
      | 6  | Loop         | 0            |                | {"loopBody": "5"}                                     |
      | 5  | Dedup        | 4            |                |                                                       |
      | 4  | Project      | 25           |                |                                                       |
      | 25 | Sample       | 26           |                |                                                       |
      | 26 | GetNeighbors | 1            |                | {"limit": "$__VAR_2[($__VAR_1-1)]", "random": "true"} |
      | 1  | Start        |              |                |                                                       |
      | 0  | Start        |              |                |                                                       |
      | 12 | Project      | 11           |                |                                                       |
      | 11 | GetVertices  | 10           |                |                                                       |
      | 10 | Argument     |              |                |                                                       |
    When profiling query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      GO 2 steps FROM $var.dst OVER like YIELD $var.dst AS dst1,like._dst AS dst2 SAMPLE [2,3]
      """
    Then the execution plan should be:
      | id | name         | dependencies | profiling data | operator info                                         |
      | 23 | Project      | 22           |                |                                                       |
      | 22 | InnerJoin    | 21           |                |                                                       |
      | 21 | InnerJoin    | 20           |                |                                                       |
      | 20 | Project      | 26           |                |                                                       |
      | 26 | Sample       | 27           |                | { "count": "3" }                                      |
      | 27 | GetNeighbors | 17           |                | {"limit": "3", "random": "true"}                      |
      | 17 | Loop         | 11           |                | {"loopBody": "16"}                                    |
      | 16 | Dedup        | 15           |                |                                                       |
      | 15 | Project      | 14           |                |                                                       |
      | 14 | InnerJoin    | 13           |                |                                                       |
      | 13 | Dedup        | 12           |                |                                                       |
      | 12 | Project      | 9            |                |                                                       |
      | 9  | Dedup        | 8            |                |                                                       |
      | 8  | Project      | 34           |                |                                                       |
      | 34 | Sample       | 35           |                | {"count": "$__VAR_2[($__VAR_1-1)]"}                   |
      | 35 | GetNeighbors | 5            |                | {"limit": "$__VAR_2[($__VAR_1-1)]", "random": "true"} |
      | 5  | Start        |              |                |                                                       |
      | 11 | Dedup        | 10           |                |                                                       |
      | 10 | Project      | 4            |                |                                                       |
      | 4  | Dedup        | 3            |                |                                                       |
      | 3  | Project      | 2            |                |                                                       |
      | 2  | Project      | 1            |                |                                                       |
      | 1  | GetNeighbors | 0            |                |                                                       |
      | 0  | Start        |              |                |                                                       |

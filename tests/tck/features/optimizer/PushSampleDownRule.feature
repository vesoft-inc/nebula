# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
@jmq
Feature: Push Limit down rule

  Background:
    Given a graph with space named "nba"

  Scenario: push limit down to Expand
    When profiling query:
      """
      GO 1 STEPS FROM "James Harden" OVER like REVERSELY YIELD like._dst LIMIT [2]
      """
    Then the result should be, in any order:
      | like._dst         |
      | "Dejounte Murray" |
      | "Luka Doncic"     |
    And the execution plan should be:
      | id | name      | dependencies | operator info         |
      | 4  | Project   | 3            |                       |
      | 3  | ExpandAll | 2            | {"stepLimits": ["2"]} |
      | 2  | Expand    | 1            | {"stepLimits": ["2"]} |
      | 1  | Start     |              |                       |
    When profiling query:
      """
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY YIELD like._dst LIMIT [2, 2]
      """
    Then the result should be, in any order:
      | like._dst            |
      | "Kristaps Porzingis" |
      | "Dejounte Murray"    |
    And the execution plan should be:
      | id | name      | dependencies | operator info              |
      | 4  | Project   | 3            |                            |
      | 3  | ExpandAll | 2            | {"stepLimits": ["2", "2"]} |
      | 2  | Expand    | 1            | {"stepLimits": ["2", "2"]} |
      | 1  | Start     |              |                            |
    When profiling query:
      """
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY YIELD $^.player.name AS src, like.likeness AS likeness, $$.player.name AS dst LIMIT [2, 2]
      """
    Then the result should be, in any order:
      | src                 | likeness | dst                  |
      | "Luka Doncic"       | 90       | "Kristaps Porzingis" |
      | "Russell Westbrook" | 99       | "Dejounte Murray"    |
    And the execution plan should be:
      | id | name         | dependencies | profiling data | operator info                                 |
      | 8  | Project      | 7            |                |                                               |
      | 7  | HashLeftJoin | 3,6          |                |                                               |
      | 3  | ExpandAll    | 2            |                | {"stepLimits": ["2", "2"], "sample": "false"} |
      | 2  | Expand       | 1            |                | {"stepLimits": ["2", "2"], "sample": "false"} |
      | 1  | Start        |              |                |                                               |
      | 6  | Project      | 5            |                |                                               |
      | 5  | GetVertices  | 4            |                |                                               |
      | 4  | Argument     |              |                |                                               |
    When profiling query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      GO 2 steps FROM $var.dst OVER like YIELD $var.dst AS dst1,like._dst AS dst2 LIMIT [2,3] | YIELD count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 4        |
    And the execution plan should be:
      | id | name          | dependencies | profiling data | operator info                                 |
      | 11 | Aggregate     | 10           |                |                                               |
      | 10 | Project       | 9            |                |                                               |
      | 9  | HashInnerJoin | 4,8          |                |                                               |
      | 4  | Project       | 3            |                |                                               |
      | 3  | ExpandAll     | 2            |                |                                               |
      | 2  | Expand        | 1            |                |                                               |
      | 1  | Start         |              |                |                                               |
      | 8  | ExpandAll     | 7            |                | {"stepLimits": ["2", "3"], "sample": "false"} |
      | 7  | Expand        | 6            |                | {"stepLimits": ["2", "3"], "sample": "false"} |
      | 6  | Argument      |              |                |                                               |

  Scenario: push sample down to Expand
    When profiling query:
      """
      GO 1 STEPS FROM "James Harden" OVER like REVERSELY YIELD like._dst SAMPLE [2]
      """
    Then the result should be, in any order:
      | like._dst |
      | /[\w\s]+/ |
      | /[\w\s]+/ |
    And the execution plan should be:
      | id | name      | dependencies | operator info                           |
      | 4  | Project   | 3            |                                         |
      | 3  | ExpandAll | 2            | {"stepLimits": ["2"], "sample": "true"} |
      | 2  | Expand    | 1            | {"stepLimits": ["2"], "sample": "true"} |
      | 1  | Start     |              |                                         |
    When profiling query:
      """
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY YIELD like._dst SAMPLE [2, 2]
      """
    Then the result should be, in any order:
      | like._dst |
      | /[\w\s]+/ |
      | /[\w\s]+/ |
    And the execution plan should be:
      | id | name      | dependencies | operator info                                |
      | 4  | Project   | 3            |                                              |
      | 3  | ExpandAll | 2            | {"stepLimits": ["2", "2"], "sample": "true"} |
      | 2  | Expand    | 1            | {"stepLimits": ["2", "2"], "sample": "true"} |
      | 1  | Start     |              |                                              |
    When profiling query:
      """
      GO 2 STEPS FROM "James Harden" OVER like REVERSELY YIELD $^.player.name AS src, like.likeness AS likeness, $$.player.name AS dst SAMPLE [2, 2]
      """
    Then the result should be, in any order:
      | src       | likeness | dst       |
      | /[\w\s]+/ | /\d\d/   | /[\w\s]+/ |
      | /[\w\s]+/ | /\d\d/   | /[\w\s]+/ |
    And the execution plan should be:
      | id | name         | dependencies | profiling data | operator info                                |
      | 8  | Project      | 7            |                |                                              |
      | 7  | HashLeftJoin | 3,6          |                |                                              |
      | 3  | ExpandAll    | 2            |                | {"stepLimits": ["2", "2"], "sample": "true"} |
      | 2  | Expand       | 1            |                | {"stepLimits": ["2", "2"], "sample": "true"} |
      | 1  | Start        |              |                |                                              |
      | 6  | Project      | 5            |                |                                              |
      | 5  | GetVertices  | 4            |                |                                              |
      | 4  | Argument     |              |                |                                              |
    When profiling query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      GO 2 steps FROM $var.dst OVER like YIELD $var.dst AS dst1,like._dst AS dst2 SAMPLE [2,3]
      """
    Then the execution plan should be:
      | id | name          | dependencies | profiling data | operator info                                |
      | 10 | Project       | 9            |                |                                              |
      | 9  | HashInnerJoin | 4,8          |                |                                              |
      | 4  | Project       | 3            |                |                                              |
      | 3  | ExpandAll     | 2            |                |                                              |
      | 2  | Expand        | 1            |                |                                              |
      | 1  | Start         |              |                |                                              |
      | 8  | ExpandAll     | 7            |                | {"stepLimits": ["2", "3"], "sample": "true"} |
      | 7  | Expand        | 6            |                | {"stepLimits": ["2", "3"], "sample": "true"} |
      | 6  | Argument      |              |                |                                              |

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push EFilter down rule
  Examples:
    | space_name  |
    | nba         |
    | nba_int_vid |

  Background: Prepare space
    Given a graph with space named "<space_name>"

  Scenario Outline: push eFilter down
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like{likeness: 95}]->() return v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name      | dependencies | operator info                                        |
      | 5  | Project   | 8            |                                                      |
      | 8  | Traverse  | 7            | {"edge filter": "", "filter": "(like.likeness==95)"} |
      | 7  | IndexScan | 0            |                                                      |
      | 0  | Start     |              |                                                      |
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*1..2{likeness: 95}]->() return v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
      | "Tim Duncan" |
      | "Tim Duncan" |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name      | dependencies | operator info                                        |
      | 5  | Project   | 8            |                                                      |
      | 8  | Traverse  | 7            | {"edge filter": "", "filter": "(like.likeness==95)"} |
      | 7  | IndexScan | 0            |                                                      |
      | 0  | Start     |              |                                                      |

  Scenario Outline: can't push eFilter down when zero step enabled
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..1{likeness: 95}]->() return v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
      | "Tim Duncan" |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name      | dependencies | operator info                                     |
      | 5  | Project   | 8            |                                                   |
      | 8  | Traverse  | 7            | {"edge filter": "(*.likeness==95)", "filter": ""} |
      | 7  | IndexScan | 0            |                                                   |
      | 0  | Start     |              |                                                   |

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Fix match losing undefined vertex tag info
  Examples:
    | space       | vid                |
    | nba         | "Tim Duncan"       |
    | nba_int_vid | hash("Tim Duncan") |

  Background: Prepare a new space with nba loaded and insert an empty tag
    Given an empty graph
    And load "<space>" csv data to a new space
    And having executed:
      """
      CREATE TAG IF NOT EXISTS empty_tag();
      """
    And having executed:
      """
      INSERT VERTEX empty_tag() values <vid>:()
      """
    And wait 3 seconds

  Scenario Outline: single vertex
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})
      RETURN labels(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                              |
      | ["player", "empty_tag", "bachelor"] |
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})
      RETURN tags(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                              |
      | ["player", "empty_tag", "bachelor"] |
    And drop the used space

  Scenario Outline: one step with direction
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-->()
      RETURN labels(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                              |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-->()
      RETURN tags(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                              |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
    And drop the used space

  Scenario Outline: one step without direction
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})--()
      RETURN labels(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                              |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})--()
      RETURN tags(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                              |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
    And drop the used space

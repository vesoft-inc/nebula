# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Collapse Project Rule

  Background:
    Given a graph with space named "nba"

  Scenario: Collapse Project
    When profiling query:
      """
      MATCH (v:player)
      WHERE v.player.age > 38
      WITH v, v.player.age AS age, v.player.age/10 AS iage, v.player.age%10 AS mage,v.player.name AS name
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
      | id | name           | dependencies | operator info |
      | 12 | Project        | 10           |               |
      | 10 | Filter         | 2            |               |
      | 2  | AppendVertices | 7            |               |
      | 7  | IndexScan      | 0            |               |
      | 0  | Start          |              |               |
    When profiling query:
      """
      LOOKUP ON player WHERE player.name=='Tim Duncan' YIELD id(vertex) as id
      | YIELD $-.id AS vid
      """
    Then the result should be, in any order:
      | vid          |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name               | dependencies | operator info |
      | 6  | Project            | 5            |               |
      | 5  | TagIndexPrefixScan | 0            |               |
      | 0  | Start              |              |               |

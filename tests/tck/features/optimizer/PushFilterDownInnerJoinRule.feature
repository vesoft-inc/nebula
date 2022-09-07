# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down InnerJoin rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down InnerJoin
    When profiling query:
      """
      LOOKUP ON player WHERE player.name == "Tony Parker"
      YIELD id(vertex) as id |
      GO FROM $-.id OVER like
      WHERE (like.likeness - 1) >= 0
      YIELD like._src AS src_id, like._dst AS dst_id, like.likeness AS likeness
      """
    Then the result should be, in any order:
      | src_id        | dst_id              | likeness |
      | "Tony Parker" | "LaMarcus Aldridge" | 90       |
      | "Tony Parker" | "Manu Ginobili"     | 95       |
      | "Tony Parker" | "Tim Duncan"        | 95       |
    And the execution plan should be:
      | id | name               | dependencies | operator info |
      | 10 | Project            | 15           |               |
      | 15 | InnerJoin          | 17           |               |
      | 17 | Project            | 18           |               |
      | 18 | GetNeighbors       | 3            |               |
      | 3  | Project            | 11           |               |
      | 11 | TagIndexPrefixScan | 0            |               |
      | 0  | Start              |              |               |

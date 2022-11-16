# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test predication push down in go

  Background:
    Given a graph with space named "nba"

  # #4838
  Scenario: predication push down in go
    When profiling query:
      """
      GO FROM "Tim Duncan" OVER like WHERE like._dst NOT IN ["xxx"]
      YIELD like._dst as dst |
      GO FROM $-.dst OVER like REVERSELY WHERE like._dst IN ["Tim Duncan"]
      YIELD like._dst AS dst;
      """
    Then the result should be, in any order:
      | dst          |
      | "Tim Duncan" |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name         | dependencies | operator info                                                                     |
      | 10 | Project      | 15           |                                                                                   |
      | 15 | InnerJoin    | 17           |                                                                                   |
      | 17 | Project      | 18           |                                                                                   |
      | 18 | GetNeighbors | 3            | { "filter": "((like._dst IN ["Tim Duncan"]) AND (like._dst IN ["Tim Duncan"]))" } |
      | 3  | Project      | 11           |                                                                                   |
      | 11 | GetNeighbors | 0            | { "filter": "((like._dst NOT IN ["xxx"]) AND (like._dst NOT IN ["xxx"]))" }       |
      | 0  | Start        |              |                                                                                   |

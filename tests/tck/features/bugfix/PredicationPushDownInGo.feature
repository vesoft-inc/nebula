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
      | id | name          | dependencies | operator info                                   |
      | 12 | Project       | 16           |                                                 |
      | 16 | HashInnerJoin | 5,17         |                                                 |
      | 5  | Project       | 13           |                                                 |
      | 13 | ExpandAll     | 2            | { "filter": "(like._dst NOT IN [\"xxx\"])" }    |
      | 2  | Expand        | 1            |                                                 |
      | 1  | Start         |              |                                                 |
      | 17 | ExpandAll     | 8            | { "filter": "(like._dst IN [\"Tim Duncan\"])" } |
      | 8  | Expand        | 7            |                                                 |
      | 7  | Argument      |              |                                                 |

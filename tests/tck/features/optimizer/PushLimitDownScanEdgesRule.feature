# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Limit down scan edges rule

  Background:
    Given a graph with space named "student"

  Scenario: push limit down to ScanEdges
    When profiling query:
      """
      MATCH ()-[e:is_teacher]->()
      RETURN e.start_year LIMIT 3
      """
    Then the result should be, in any order:
      | e.start_year |
      | /\d+/        |
      | /\d+/        |
      | /\d+/        |
    And the execution plan should be:
      | id | name           | dependencies | operator info  |
      | 19 | Project        | 16           |                |
      | 16 | Limit          | 11           |                |
      | 11 | AppendVertices | 3            |                |
      | 3  | Project        | 2            |                |
      | 2  | ScanEdges      | 0            | {"limit": "3"} |
      | 0  | Start          |              |                |
    When profiling query:
      """
      MATCH ()-[e]->()
      RETURN type(e) LIMIT 3
      """
    Then the result should be, in any order:
      | type(e) |
      | /\w+/   |
      | /\w+/   |
      | /\w+/   |
    And the execution plan should be:
      | id | name           | dependencies | operator info  |
      | 19 | Project        | 16           |                |
      | 16 | Limit          | 11           |                |
      | 11 | AppendVertices | 3            |                |
      | 3  | Project        | 2            |                |
      | 2  | ScanEdges      | 0            | {"limit": "3"} |
      | 0  | Start          |              |                |

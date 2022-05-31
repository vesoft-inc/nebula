# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Limit down scan edges rule

  Background:
    Given a graph with space named "student"

  Scenario: Not optimized to ScanEdges
    When profiling query:
      """
      MATCH p=()-[e]->()
      RETURN p LIMIT 3
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    And the execution plan should be:
      | id | name           | dependencies | operator info   |
      | 15 | Project        | 13           |                 |
      | 13 | Limit          | 4            |                 |
      | 4  | Traverse       | 12           |                 |
      | 12 | ScanVertices   | 3            | {"limit": "-1"} |
      | 3  | Start          |              |                 |

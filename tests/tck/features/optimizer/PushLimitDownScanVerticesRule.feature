# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Limit down scan vertices rule

  Background:
    Given a graph with space named "student"

  Scenario: push limit down to ScanVertices
    When profiling query:
      """
      MATCH (v)
      RETURN v.person.name LIMIT 3
      """
    Then the result should be, in any order:
      | v.person.name |
      | /\w+/         |
      | /\w+/         |
      | /\w+/         |
    And the execution plan should be:
      | id | name           | dependencies | operator info  |
      | 19 | Project        | 16           |                |
      | 16 | Limit          | 11           |                |
      | 11 | AppendVertices | 2            |                |
      | 2  | ScanVertices   | 0            | {"limit": "3"} |
      | 0  | Start          |              |                |
    When profiling query:
      """
      MATCH (v:person)
      RETURN v.person.name LIMIT 3
      """
    Then the result should be, in any order:
      | v.person.name |
      | /\w+/         |
      | /\w+/         |
      | /\w+/         |
    And the execution plan should be:
      | id | name           | dependencies | operator info  |
      | 19 | Project        | 16           |                |
      | 16 | Limit          | 11           |                |
      | 11 | AppendVertices | 2            |                |
      | 2  | ScanVertices   | 0            | {"limit": "3"} |
      | 0  | Start          |              |                |

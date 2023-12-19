# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test PrunePropertiesVisitor when switching space

  Scenario: Test PrunePropertiesVisitor when switching space
    When executing query:
      """
      USE student;
      """
    Then the execution should be successful
    When profiling query:
      """
      USE nba;
      MATCH (u:player)
      RETURN count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 56       |
    And the execution plan should be:
      | id | name                   | dependencies | operator info                            |
      | 6  | Aggregate              | 8            |                                          |
      | 8  | AppendVertices         | 2            | {  "props": "[{\"props\":[\"_tag\"]}]" } |
      | 2  | IndexScan              | 1            |                                          |
      | 1  | RegisterSpaceToSession | 0            |                                          |
      | 0  | Start                  |              |                                          |

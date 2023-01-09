# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test lack filter of get edges transform

  Background:
    Given a graph with space named "nba"

  # #5145
  Scenario: Lack filter of get edges transform
    When profiling query:
      """
      match ()-[e]->()
      where e.likeness > 78  or uuid() > 100
      return rank(e) limit 3
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    And the execution plan should be:
      | id | name           | dependencies | operator info                                         |
      | 24 | Project        | 20           |                                                       |
      | 20 | Limit          | 21           |                                                       |
      | 21 | AppendVertices | 23           |                                                       |
      | 23 | Traverse       | 22           | { "edge filter": "((*.likeness>78) OR (uuid()>100))"} |
      | 22 | ScanVertices   | 3            |                                                       |
      | 3  | Start          |              |                                                       |

# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down HashInnerJoin rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down HashInnerJoin
    When profiling query:
      """
      with ['Tim Duncan', 'Tony Parker'] as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in ['Tim Duncan', 'Tony Parker'] AND id(v2) in ['Tim Duncan', 'Tony Parker']
      return count(e)
      """
    Then the result should be, in any order:
      | count(e) |
      | 8        |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                    |
      | 11 | Aggregate      | 14           |                                                                  |
      | 14 | CrossJoin      | 1,16         |                                                                  |
      | 1  | Project        | 2            |                                                                  |
      | 2  | Start          |              |                                                                  |
      | 16 | Project        | 15           |                                                                  |
      | 15 | Filter         | 18           | {"condition": "(id($-.v2) IN [\"Tim Duncan\",\"Tony Parker\"])"} |
      | 18 | AppendVertices | 17           |                                                                  |
      | 17 | Filter         | 17           | {"condition": "(id($-.v1) IN [\"Tim Duncan\",\"Tony Parker\"])"} |
      | 17 | Traverse       | 4            |                                                                  |
      | 4  | Dedup          | 3            |                                                                  |
      | 3  | PassThrough    | 5            |                                                                  |
      | 5  | Start          |              |                                                                  |

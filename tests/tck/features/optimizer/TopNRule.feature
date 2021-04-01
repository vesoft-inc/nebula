# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: TopN rule

  Background:
    Given a graph with space named "nba"

  Scenario: apply topn opt rule
    When profiling query:
      """
      GO 1 STEPS FROM "Marco Belinelli" OVER like
      YIELD like.likeness AS likeness |
      ORDER BY likeness |
      LIMIT 2
      """
    Then the result should be, in order:
      | likeness |
      | 50       |
      | 55       |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 0  | DataCollect  | 1            |               |
      | 1  | TopN         | 2            |               |
      | 2  | Project      | 3            |               |
      | 3  | GetNeighbors | 4            |               |
      | 4  | Start        |              |               |

  Scenario: apply topn opt rule with reverse traversal
    When profiling query:
      """
      GO 1 STEPS FROM "Marco Belinelli" OVER like REVERSELY
      YIELD like.likeness AS likeness |
      ORDER BY likeness |
      LIMIT 1
      """
    Then the result should be, in order:
      | likeness |
      | 83       |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 0  | DataCollect  | 1            |               |
      | 1  | TopN         | 2            |               |
      | 2  | Project      | 3            |               |
      | 3  | GetNeighbors | 4            |               |
      | 4  | Start        |              |               |

  Scenario: [1] fail to apply topn rule
    When profiling query:
      """
      GO 1 STEPS FROM "Marco Belinelli" OVER like
      YIELD like.likeness as likeness |
      ORDER BY likeness |
      LIMIT 2, 3
      """
    Then the result should be, in order:
      | likeness |
      | 60       |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 0  | DataCollect  | 1            |               |
      | 1  | Limit        | 2            |               |
      | 2  | Sort         | 3            |               |
      | 3  | Project      | 4            |               |
      | 4  | GetNeighbors | 5            |               |
      | 5  | Start        |              |               |

  Scenario: [2] fail to apply topn rule
    When profiling query:
      """
      GO 1 STEPS FROM "Marco Belinelli" OVER like
      YIELD like.likeness AS likeness |
      ORDER BY likeness
      """
    Then the result should be, in any order:
      | likeness |
      | 50       |
      | 55       |
      | 60       |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 0  | DataCollect  | 1            |               |
      | 1  | Sort         | 2            |               |
      | 2  | Project      | 3            |               |
      | 3  | GetNeighbors | 4            |               |
      | 4  | Start        |              |               |

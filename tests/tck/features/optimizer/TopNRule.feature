# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: TopN rule

  Background:
    Given a graph with space named "nba"

  Scenario: apply topn opt rule
    When profiling query:
      """
      GO 1 STEPS FROM "Marco Belinelli" OVER like
      YIELD like.likeness AS likeness |
      ORDER BY $-.likeness |
      LIMIT 2
      """
    Then the result should be, in order:
      | likeness |
      | 50       |
      | 55       |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 7  | TopN      | 4            |               |
      | 4  | Project   | 3            |               |
      | 3  | ExpandAll | 2            |               |
      | 2  | Expand    | 1            |               |
      | 1  | Start     |              |               |

  Scenario: apply topn opt rule with reverse traversal
    When profiling query:
      """
      GO 1 STEPS FROM "Marco Belinelli" OVER like REVERSELY
      YIELD like.likeness AS likeness |
      ORDER BY $-.likeness |
      LIMIT 1
      """
    Then the result should be, in order:
      | likeness |
      | 83       |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 7  | TopN      | 4            |               |
      | 4  | Project   | 3            |               |
      | 3  | ExpandAll | 2            |               |
      | 2  | Expand    | 1            |               |
      | 1  | Start     |              |               |

  Scenario: [1] fail to apply topn rule
    When profiling query:
      """
      GO 1 STEPS FROM "Marco Belinelli" OVER like
      YIELD like.likeness as likeness |
      ORDER BY $-.likeness |
      LIMIT 2, 3
      """
    Then the result should be, in order:
      | likeness |
      | 60       |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 6  | Limit     | 5            |               |
      | 5  | Sort      | 4            |               |
      | 4  | Project   | 3            |               |
      | 3  | ExpandAll | 2            |               |
      | 2  | Expand    | 1            |               |
      | 1  | Start     |              |               |

  Scenario: [2] fail to apply topn rule
    When profiling query:
      """
      GO 1 STEPS FROM "Marco Belinelli" OVER like
      YIELD like.likeness AS likeness |
      ORDER BY $-.likeness
      """
    Then the result should be, in any order:
      | likeness |
      | 50       |
      | 55       |
      | 60       |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 5  | Sort      | 4            |               |
      | 4  | Project   | 3            |               |
      | 3  | ExpandAll | 2            |               |
      | 2  | Expand    | 1            |               |
      | 1  | Start     |              |               |

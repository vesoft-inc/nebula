# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Push Filter down GetNeighbors rule

  Background:
    Given a graph with space named "nba"

  Scenario: push start vertex filter down
    When profiling query:
      """
      GO 1 STEPS FROM "Boris Diaw" OVER serve
      WHERE $^.player.age > 18
      YIELD serve.start_year as start_year
      """
    Then the result should be, in any order:
      | start_year |
      | 2003       |
      | 2005       |
      | 2008       |
      | 2012       |
      | 2016       |
    And the execution plan should be:
      | name         | dependencies | operator info              |
      | Project      | 1            |                            |
      | GetNeighbors | 2            | filter: ($^.player.age>18) |
      | Start        |              |                            |

  Scenario: push start vertex filter down when reversely
    When profiling query:
      """
      GO 1 STEPS FROM "James Harden" OVER like REVERSELY
      WHERE $^.player.age > 18
      YIELD like.likeness as likeness
      """
    Then the result should be, in any order:
      | likeness |
      | 80       |
      | 90       |
      | 99       |
    And the execution plan should be:
      | name         | dependencies | operator info              |
      | Project      | 1            |                            |
      | GetNeighbors | 2            | filter: ($^.player.age>18) |
      | Start        |              |                            |

  Scenario: push edge props filter down
    When profiling query:
      """
      GO 1 STEPS FROM "Boris Diaw" OVER serve
      WHERE serve.start_year > 2005
      YIELD serve.start_year as start_year
      """
    Then the result should be, in any order:
      | start_year |
      | 2008       |
      | 2012       |
      | 2016       |
    And the execution plan should be:
      | name         | dependencies | operator info                   |
      | Project      | 1            |                                 |
      | GetNeighbors | 2            | filter: (serve.start_year>2005) |
      | Start        |              |                                 |

  Scenario: push edge props filter down when reversely
    When profiling query:
      """
      GO 1 STEPS FROM "Lakers" OVER serve REVERSELY
      WHERE serve.start_year < 2017
      YIELD serve.start_year as start_year
      """
    Then the result should be, in any order:
      | start_year |
      | 2012       |
      | 1996       |
      | 1996       |
      | 2008       |
      | 2012       |
    And the execution plan should be:
      | name         | dependencies | operator info                   |
      | Project      | 1            |                                 |
      | GetNeighbors | 2            | filter: (serve.start_year<2017) |
      | Start        |              |                                 |

  @skip
  Scenario: Only push start vertex filter down
    When profiling query:
      """
      GO 1 STEPS FROM "Boris Diaw" OVER serve
      WHERE $^.player.age > 18 AND $$.team.name == "Lakers"
      YIELD $^.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Boris Diaw" |
    And the execution plan should be:
      | name         | dependencies | operator info                       |
      | Project      | 1            |                                     |
      | Filter       | 2            | condition: ($$.team.name=="Lakers") |
      | GetNeighbors | 3            | filter: ($^.player.age>18)          |
      | Start        |              |                                     |

  @skip
  Scenario: fail to push start or end vertex filter condition down
    When profiling query:
      """
      GO 1 STEPS FROM "Boris Diaw" OVER serve
      WHERE $^.player.age > 18 OR $$.team.name == "Lakers"
      YIELD $^.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Boris Diaw" |
    And the execution plan should be:
      | name         | dependencies | operator info                                             |
      | Project      | 1            |                                                           |
      | Filter       | 2            | condition: ($^.player.age>18) OR ($$.team.name=="Lakers") |
      | GetNeighbors | 3            |                                                           |
      | Start        |              |                                                           |

  @skip
  Scenario: fail to push end vertex filter down
    When profiling query:
      """
      GO 1 STEPS FROM "Boris Diaw" OVER serve
      WHERE $$.team.name == "Lakers"
      YIELD $^.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Boris Diaw" |
    And the execution plan should be:
      | name         | dependencies | operator info |
      | Project      | 1            |               |
      | Filter       | 2            |               |
      | GetNeighbors | 3            |               |
      | Start        |              |               |

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
      | id | name         | dependencies | operator info                    |
      | 0  | Project      | 1            |                                  |
      | 1  | GetNeighbors | 2            | {"filter": "($^.player.age>18)"} |
      | 2  | Start        |              |                                  |

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
      | id | name         | dependencies | operator info                    |
      | 0  | Project      | 1            |                                  |
      | 1  | GetNeighbors | 2            | {"filter": "($^.player.age>18)"} |
      | 2  | Start        |              |                                  |

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
      | id | name         | dependencies | operator info                         |
      | 0  | Project      | 1            |                                       |
      | 1  | GetNeighbors | 2            | {"filter": "(serve.start_year>2005)"} |
      | 2  | Start        |              |                                       |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE like.likeness IN [v IN [95,99] WHERE v > 0]
      YIELD like._dst, like.likeness
      """
    Then the result should be, in any order:
      | like._dst       | like.likeness |
      | "Manu Ginobili" | 95            |
      | "Tim Duncan"    | 95            |
    And the execution plan should be:
      | id | name         | dependencies | operator info                                                            |
      | 0  | Project      | 1            |                                                                          |
      | 1  | GetNeighbors | 2            | {"filter": "(like.likeness IN [__VAR_0 IN [95,99] WHERE ($__VAR_0>0)])"} |
      | 2  | Start        |              |                                                                          |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE any(v in [5,6] WHERE like.likeness + v < 100)
      YIELD like._dst, like.likeness
      """
    Then the result should be, in any order:
      | like._dst           | like.likeness |
      | "LaMarcus Aldridge" | 90            |
    And the execution plan should be:
      | id | name         | dependencies | operator info                                                            |
      | 0  | Project      | 1            |                                                                          |
      | 1  | GetNeighbors | 2            | {"filter": "any(__VAR_0 IN [5,6] WHERE ((like.likeness+$__VAR_0)<100))"} |
      | 2  | Start        |              |                                                                          |

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
      | id | name         | dependencies | operator info                         |
      | 0  | Project      | 1            |                                       |
      | 1  | GetNeighbors | 2            | {"filter": "(serve.start_year<2017)"} |
      | 2  | Start        |              |                                       |

  @skip
  Scenario: Only push source vertex filter down
    When profiling query:
      """
      GO 1 STEPS FROM "Boris Diaw" OVER serve
      WHERE $^.player.age > 18 AND $$.team.name == "Spurs"
      YIELD $^.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Boris Diaw" |
    And the execution plan should be:
      | name         | dependencies | operator info                            |
      | Project      | 1            |                                          |
      | Filter       | 2            | {"condition": "($$.team.name=="Spurs")"} |
      | GetNeighbors | 3            | {"filter": "($^.player.age>18)"}         |
      | Start        |              |                                          |

  @skip
  Scenario: fail to push source or dst vertex filter condition down
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
      | name         | dependencies | operator info                                                   |
      | Project      | 1            |                                                                 |
      | Filter       | 2            | {"condition": "($^.player.age>18) OR ($$.team.name=="Lakers")"} |
      | GetNeighbors | 3            |                                                                 |
      | Start        |              |                                                                 |

  @skip
  Scenario: fail to push dst vertex filter down
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

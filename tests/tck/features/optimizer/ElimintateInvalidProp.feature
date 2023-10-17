# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Eliminate invalid property filter
  Examples:
     | space_name  |
     | nba         |
     | nba_int_vid |

  Background:
    Given a graph with space named "<space_name>"

  Scenario: Elimintate Not Exists Tag
    When profiling query:
      """
      MATCH (v:player) WHERE v.not_exists_tag.name == 'Tim Duncan' RETURN v.player.age AS age
      """
    Then the result should be, in any order:
      | age |
    And the execution plan should be:
      | id | name    | dependencies | operator info |
      | 0  | Project | 1            |               |
      | 1  | Value   | 2            |               |
      | 2  | Start   |              |               |

  Scenario: Elimintate Not Exists Property
    When profiling query:
      """
      MATCH (v:player) WHERE v.player.not_exists_prop == 'Tim Duncan' RETURN v.player.age AS age
      """
    Then the result should be, in any order:
      | age |
    And the execution plan should be:
      | id | name    | dependencies | operator info |
      | 0  | Project | 1            |               |
      | 1  | Value   | 2            |               |
      | 2  | Start   |              |               |

  Scenario: Elimintate Empty value
    When executing query:
      """
      MATCH (a2:player) WHERE 28==0.63
      MATCH (b)-[e4]->(a2) WHERE 18 <= a2.player.age
      RETURN a2
      """
    Then the result should be, in any order:
      | a2 |

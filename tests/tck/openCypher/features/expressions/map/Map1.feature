# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Map1 - Static value access

  # Static value access refers to the dot-operator – <expression resulting in a map>.<identify> – which does not allow any dynamic computation of the map key – i.e. <identify>.
  Background:
    Given a graph with space named "nba"

  Scenario: [1] Statically access field of a map resulting from an expression
    When executing query:
      """
      WITH [{num: 0}, 1] AS list
      RETURN (list[0]).num
      """
    Then the result should be, in any order:
      | list[0].num |
      | 0           |

  @uncompatible
  Scenario: [2] Fail when performing property access on a non-map
    # openCyter return : TypeError should be raised at runtime: PropertyAccessOnNonMap
    When executing query:
      """
      WITH [{num: 0}, 1] AS list
      RETURN (list[1]).num
      """
    Then the result should be, in any order:
      | list[1].num |
      | BAD_TYPE    |

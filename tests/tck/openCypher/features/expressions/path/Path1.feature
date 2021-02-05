# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
@skip
# unimplement
Feature: Path1 - Nodes of a path

  Background:
    Given a graph with space named "nba"

  Scenario: [1] `nodes()` on null path
    When executing query:
      """
      WITH null AS a
      OPTIONAL MATCH p = (a)-[r]->()
      RETURN nodes(p), nodes(null)
      """
    Then the result should be, in any order, with relax comparison:
      | nodes(p) | nodes(null) |
      | null     | null        |

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test implicit bool checking in expr

  Background:
    Given a graph with space named "nba"

  Scenario: Implicit bool checking in expr
    When executing query:
      """
      MATCH (v:player) WHERE id(v) == 'Tim Duncan' RETURN [i in [v] WHERE (v)-[:like]->()] AS ret
      """
    Then the result should be, in any order, with relax comparison:
      | ret              |
      | [("Tim Duncan")] |

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Ensure one row when vertex exists 

  Background:
    Given a graph with space named "nba"

  Scenario: match
    When executing query:
      """
      MATCH (v:bachelor) RETURN v.team.name AS name
      """
    Then the result should be, in any order:
      | name |
      | NULL |

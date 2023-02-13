# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Coalesce Function

  Background:
    Given a graph with space named "nba"

  Scenario: test normal case
    When executing query:
      """
      RETURN coalesce(null,1) as result;
      """
    Then the result should be, in any order:
      | result |
      | 1      |
    When executing query:
      """
      RETURN coalesce(1,2,3) as result;
      """
    Then the result should be, in any order:
      | result |
      | 1      |
    When executing query:
      """
      RETURN coalesce(null) as result;
      """
    Then the result should be, in any order:
      | result |
      | NULL   |
    When executing query:
      """
      RETURN coalesce(null,[1,2,3]) as result;
      """
    Then the result should be, in any order:
      | result    |
      | [1, 2, 3] |
    When executing query:
      """
      RETURN coalesce(null,1.234) as result;
      """
    Then the result should be, in any order:
      | result |
      | 1.234  |

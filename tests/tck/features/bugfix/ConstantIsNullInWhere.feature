# Copyright (c) 2026 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: constant is null in where

  Background:
    Given a graph with space named "nba"

  # Regression for #6138: `WHERE 1 IS NULL` should be folded to a constant
  # `false` so that it behaves exactly like `WHERE false`, instead of being
  # executed as a non-folded filter that may crash the graphd service.
  Scenario: where with constant is null predicate
    When try to execute query:
      """
      UNWIND range(1,100) AS p WITH p WHERE 1 IS NULL RETURN p
      """
    Then the execution should be successful

  Scenario: where with constant is not null predicate
    When try to execute query:
      """
      UNWIND range(1,100) AS p WITH p WHERE 1 IS NOT NULL RETURN p
      """
    Then the execution should be successful

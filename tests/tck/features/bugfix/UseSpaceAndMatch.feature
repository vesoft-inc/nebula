# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
# Fix https://github.com/vesoft-inc/nebula/issues/5263
Feature: Use space combine with Match

  Background:
    Given a graph with space named "nba"

  # The edge range will use same hard code innner variable name for list comprehension
  # It's not stable to reproduce, so run multiple times
  Scenario: Use space combine with Match
    When executing query:
      """
      CREATE USER IF NOT EXISTS new_user WITH PASSWORD 'nebula';
      """
    And wait 6 seconds
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE ADMIN ON nba TO new_user;
      """
    Then the execution should be successful
    And wait 3 seconds
    When switch to new session with user "new_user" and password "nebula"
    And executing query:
      """
      USE nba; MATCH (p)-[e]->(v) WHERE id(p)=="Tony Parker" RETURN v.player.age
      """
    Then the result should be, in any order, with relax comparison:
      | v.player.age |
      | NULL         |
      | NULL         |
      | 25           |
      | 33           |
      | 41           |
      | 42           |
      | 33           |
      | 41           |
      | 42           |

# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
# Fix https://github.com/vesoft-inc/nebula/issues/5263
Feature: Use space combine with Match

  Scenario: Use space combine with Match
    Given an empty graph
    And load "nba" csv data to a new space
    When executing query:
      """
      CREATE USER IF NOT EXISTS new_user_5263 WITH PASSWORD 'nebula';
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE ADMIN ON nba TO new_user_5263;
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query with user "new_user_5263" and password "nebula":
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
    When executing query:
      """
      DROP USER IF EXISTS new_user_5263;
      """
    Then the execution should be successful
    Then drop the used space

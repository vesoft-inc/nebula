# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test kill queries from same service

  Background:
    Given a graph with space named "nba"

  Scenario: [slowquery_test_401] Setup slow query
    # Set up a slow query which will be killed later.
    When executing query:
      """
      GO 100001 STEPS FROM "Tim Duncan" OVER like YIELD like._dst
      """
    Then an ExecutionError should be raised at runtime: Execution had been killed

  Scenario: [slowquery_test_402] Kill successful by user root
    When executing query:
      """
      CREATE USER IF NOT EXISTS test_permission WITH PASSWORD 'test';
      GRANT ROLE USER ON nba TO test_permission;
      """
    Then the execution should be successful
    And wait 10 seconds
    # Make sure the record exists
    When executing query with user "test_permission" and password "test":
      """
      USE nba;
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100001 STEPS";
      """
    Then the result should be, in order:
      | sid        | eid   | dur   |
      | /[-+]?\d+/ | /\d+/ | /\d+/ |
    When executing query with user "test_permission" and password "test":
      """
      USE nba;
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100001 STEPS"
      | ORDER BY $-.dur
      | KILL QUERY(session=$-.sid, plan=$-.eid)
      """
    Then an PermissionError should be raised at runtime: Only GOD role could kill others' queries.
    When executing query with user "root" and password "nebula":
      """
      USE nba;
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100001 STEPS"
      | ORDER BY $-.dur
      | KILL QUERY(session=$-.sid, plan=$-.eid)
      """
    Then the execution should be successful

  Scenario: [slowquery_test_403] Setup slow query
    When executing query:
      """
      CREATE USER IF NOT EXISTS test_permission WITH PASSWORD 'test';
      GRANT ROLE USER ON nba TO test_permission;
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query with user "test_permission" and password "test":
      """
      USE nba;
      GO 100002 STEPS FROM "Tim Duncan" OVER like YIELD like._dst
      """
    Then an ExecutionError should be raised at runtime: Execution had been killed

  Scenario: [slowquery_test_404] Kill successful by user test_permission
    When executing query:
      """
      SHOW QUERIES
      """
    Then the execution should be successful
    # Make sure the record exists
    And wait 15 seconds
    When executing query with user "test_permission" and password "test":
      """
      USE nba;
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100002 STEPS";
      """
    Then the result should be, in order:
      | sid        | eid   | dur   |
      | /[-+]?\d+/ | /\d+/ | /\d+/ |
    When executing query with user "test_permission" and password "test":
      """
      USE nba;
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100002 STEPS"
      | ORDER BY $-.dur
      | KILL QUERY(session=$-.sid, plan=$-.eid)
      """
    Then the execution should be successful

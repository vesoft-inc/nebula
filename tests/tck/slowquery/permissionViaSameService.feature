# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test kill queries from same service

  Scenario: Setup slow query
    # Set up a slow query which will be killed later.
    Given a graph with space named "nba"
    When executing query:
      """
      GO 100000 STEPS FROM "Tim Duncan" OVER like YIELD like._dst
      """
    Then an ExecutionError should be raised at runtime: Execution had been killed

  Scenario: Test permisson of kill queries
    Given a graph with space named "nba"
    When executing query:
      """
      CREATE USER IF NOT EXISTS test_permission WITH PASSWORD 'test';
      GRANT ROLE USER ON nba TO test_permission;
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query with user test_permission with password test:
      """
      USE nba;
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO"
      | ORDER BY $-.dur
      | KILL QUERY(session=$-.sid, plan=$-.eid)
      """
    Then an PermissionError should be raised at runtime: Only GOD role could kill others' queries.
    When executing query with user root with password nebula:
      """
      USE nba;
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO"
      | ORDER BY $-.dur
      | KILL QUERY(session=$-.sid, plan=$-.eid)
      """
    Then the execution should be successful

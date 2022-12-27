# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Slow Query Test

  Background:
    Given a graph with space named "nba"

  # There should be a least 2 thread to run this test case suite.
  Scenario: [slowquery_test_201] Setup slow query
    # Set up a slow query which will be killed later.
    When executing query:
      """
      GO 100000 STEPS FROM "Tim Duncan" OVER like YIELD like._dst
      """
    Then an ExecutionError should be raised at runtime: Execution had been killed

  Scenario: [slowquery_test_202] kill go sentence on same service
    When executing query:
      """
      SHOW QUERIES
      """
    Then the execution should be successful
    And wait 10 seconds
    # make sure the record exists
    When executing query:
      """
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100000 STEPS";
      """
    Then the result should be, in order:
      | sid   | eid   | dur   |
      | /\d+/ | /\d+/ | /\d+/ |
    When executing query:
      """
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100000 STEPS"
      | ORDER BY $-.dur
      | KILL QUERY(session=$-.sid, plan=$-.eid)
      """
    Then the execution should be successful

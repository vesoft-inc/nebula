# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Slow Query Test

  Background:
    Given a graph with space named "nba"

  # There should be a least 2 thread to run this test case suite.
  Scenario: [slowquery_test_101] Setup slow query
    # Set up a slow query which will be killed later.
    When executing query via graph 0:
      """
      GO 100000 STEPS FROM "Tim Duncan" OVER like YIELD like._dst
      """
    Then an ExecutionError should be raised at runtime: Execution had been killed

  Scenario: [slowquery_test_102] Kill go sentence
    When executing query via graph 1:
      """
      SHOW QUERIES
      """
    Then the execution should be successful
    # make sure the record exists
    And wait 10 seconds
    When executing query via graph 1:
      """
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100000 STEPS";
      """
    Then the result should be, in order:
      | sid        | eid   | dur   |
      | /[-+]?\d+/ | /\d+/ | /\d+/ |
    # sessionId not exist
    When executing query via graph 1:
      """
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100000 STEPS"
      | ORDER BY $-.dur
      | KILL QUERY(session=200, plan=$-.eid)
      """
    Then an ExecutionError should be raised at runtime: SessionId[200] does not exist
    # planId not exist
    When executing query via graph 1:
      """
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100000 STEPS"
      | ORDER BY $-.dur
      | KILL QUERY(session=$-.sid, plan=201)
      """
    Then an ExecutionError should be raised at runtime.
    # Kill go sentence
    When executing query via graph 1:
      """
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.ExecutionPlanID AS eid, $-.DurationInUSec AS dur
      WHERE $-.DurationInUSec > 1000000 AND $-.`Query` CONTAINS "GO 100000 STEPS"
      | ORDER BY $-.dur
      | KILL QUERY(session=$-.sid, plan=$-.eid)
      """
    Then the execution should be successful

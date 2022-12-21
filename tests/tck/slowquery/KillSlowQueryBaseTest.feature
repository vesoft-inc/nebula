# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Slow Query Test

  Background:
    Given a graph with space named "nba"

  Scenario: [slowquery_test_001] without sessionId and planId
    When executing query:
      """
      KILL QUERY ()
      """
    Then a SyntaxError should be raised at runtime: syntax error near `)'

  Scenario: [slowquery_test_002] without sessionId
    When executing query:
      """
      KILL QUERY (session=123)
      """
    Then a SyntaxError should be raised at runtime: syntax error near `)'

  Scenario: [slowquery_test_003] without planId
    When executing query:
      """
      KILL QUERY (plan=987654321)
      """
    Then an ExecutionError should be raised at runtime: ExecutionPlanId[987654321] does not exist in current Session.

  Scenario: [slowquery_test_004] wrong sessionId and planId
    When executing query:
      """
      KILL QUERY (session=987654321, plan=987654321)
      """
    Then an ExecutionError should be raised at runtime: SessionId[987654321] does not exist

  Scenario: [slowquery_test_005] sessionId is STRING
    When executing query:
      """
      KILL QUERY (session="100", plan=101)
      """
    Then a SyntaxError should be raised at runtime: syntax error near `", plan='

  Scenario: [slowquery_test_006] planId is STRING
    When executing query:
      """
      KILL QUERY (session=100, plan="101")
      """
    Then a SyntaxError should be raised at runtime: syntax error near `")'

  Scenario: [slowquery_test_007] sessionId and planId are STRING
    When executing query:
      """
      KILL QUERY (session="100", plan="101")
      """
    Then a SyntaxError should be raised at runtime: syntax error near `", plan='

  Scenario: [slowquery_test_008] wrong sessionId
    When executing query:
      """
      KILL QUERY (session=$-.sid)
      """
    Then an SyntaxError should be raised at runtime: syntax error near `)'

  Scenario: [slowquery_test_009] wrong planId
    When executing query:
      """
      KILL QUERY (plan=$-.eid)
      """
    Then an SemanticError should be raised at runtime: `$-.eid', not exist prop `eid'

  Scenario: [slowquery_test_010] wrong sessionId and planId
    When executing query:
      """
      KILL QUERY (session=$-.sid, plan=$-.eid)
      """
    Then an SemanticError should be raised at runtime: `$-.sid', not exist prop `sid'

  Scenario: [slowquery_test_011] show queries
    When executing query:
      """
      SHOW LOCAL QUERIES
      """
    Then the execution should be successful

  Scenario: [slowquery_test_012] show queries
    When executing query:
      """
      SHOW QUERIES
      """
    Then the execution should be successful

  Scenario: [slowquery_test_013] sessionId is string
    When executing query via graph 1:
      """
      SHOW QUERIES
      | YIELD $-.SessionID AS sid, $-.`Query` AS eid, $-.DurationInUSec AS dur WHERE $-.DurationInUSec > 10000000
      | ORDER BY $-.dur
      | KILL QUERY (session=$-.sid, plan=$-.eid)
      """
    Then an SemanticError should be raised at runtime: $-.eid, Session ID must be an integer but was STRING

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Submit job space requirements

  Scenario: submit job require space
    Given an empty graph
    When executing query:
      """
      SUBMIT JOB COMPACT;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      SUBMIT JOB FLUSH;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      SUBMIT JOB STATS;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      REBUILD TAG INDEX not_exists_index;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      REBUILD EDGE INDEX not_exists_index;
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Operate job require space:
    Given an empty graph
    When executing query:
      """
      SHOW JOB 123456;
      """
    Then a SemanticError should be raised at runtime: Space was not chosen.
    When executing query:
      """
      STOP JOB 123456;
      """
    Then a SemanticError should be raised at runtime: Space was not chosen.
    When executing query:
      """
      RECOVER JOB;
      """
    Then a SemanticError should be raised at runtime: Space was not chosen.

  Scenario: Not exist job
    Given create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And wait 6 seconds
    When executing query:
      """
      SHOW JOB 123456;
      """
    Then a ExecutionError should be raised at runtime: Job not existed in chosen space!
    When executing query:
      """
      STOP JOB 123456;
      """
    Then a ExecutionError should be raised at runtime: Job not existed in chosen space!

  Scenario: Submit and show jobs
    Given create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And wait 6 seconds
    When executing query:
      """
      SUBMIT JOB COMPACT;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 1 seconds
    When executing query:
      """
      SUBMIT JOB FLUSH;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 1 seconds
    When executing query:
      """
      SUBMIT JOB STATS;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 10 seconds
    When executing query:
      """
      SHOW JOBS;
      """
    Then the result should be, the first 3 records in order, and register Job Id as a list named job_id:
      | Job Id | Command   | Status     | Start Time | Stop Time |
      | /\d+/  | "STATS"   | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "FLUSH"   | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
    When executing query, fill replace holders with element index of 0 in job_id:
      """
      SHOW JOB {};
      """
    Then the result should be, in order:
      | Job Id(TaskId) | Command(Dest) | Status     | Start Time | Stop Time | Error Code  |
      | /\d+/          | "STATS"       | "FINISHED" | /\w+/      | /\w+/     | "SUCCEEDED" |
      | /\d+/          | /\w+/         | "FINISHED" | /\w+/      | /\w+/     | "SUCCEEDED" |
      | /\w+/          | /\w+/         | /\w+/      | /\w+/      | ""        | ""          |
    When executing query, fill replace holders with element index of 0 in job_id:
      """
      STOP JOB {};
      """
    Then an ExecutionError should be raised at runtime: Finished job or failed job can not be stopped, please start another job instead

  Scenario: Submit and show jobs in other space
    Given create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And wait 6 seconds
    When executing query:
      """
      SUBMIT JOB COMPACT;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 1 seconds
    When executing query:
      """
      SUBMIT JOB FLUSH;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 1 seconds
    When executing query:
      """
      SUBMIT JOB STATS;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 10 seconds
    When executing query:
      """
      SHOW JOBS;
      """
    Then the result should be, the first 3 records in order, and register Job Id as a list named job_id:
      | Job Id | Command   | Status     | Start Time | Stop Time |
      | /\d+/  | "STATS"   | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "FLUSH"   | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
    Given create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      SHOW JOBS;
      """
    Then the result should be, in order:
      | Job Id | Command | Status | Start Time | Stop Time |
    When executing query, fill replace holders with element index of 0 in job_id:
      """
      SHOW JOB {};
      """
    Then an ExecutionError should be raised at runtime:Job not existed in chosen space!
    When executing query, fill replace holders with element index of 0 in job_id:
      """
      STOP JOB {};
      """
    Then an ExecutionError should be raised at runtime:Job not existed in chosen space!

  # This is skipped because it is hard to simulate the situation
  # When executing query:
  # """
  # RECOVER JOB;
  # """
  # Then the result should be successful
  Scenario: rebuild index after use, fix #2806
    When executing query:
      """
      USE nba;
      REBUILD TAG INDEX;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |

@job
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

  Scenario: Not existed job
    Given an empty graph
    When executing query:
      """
      SHOW JOB 123456;
      """
    Then a ExecutionError should be raised at runtime: Key not existed!
    When executing query:
      """
      STOP JOB 123456;
      """
    Then a ExecutionError should be raised at runtime: Key not existed!

  Scenario: Submit and show jobs
    Given create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
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
      | Job Id(TaskId) | Command(Dest) | Status     | Start Time | Stop Time |
      | /\d+/          | "STATS"       | /\^"FINISHED"/ | /\w+/      | /\w+/     |
      | /\d+/          | /\w+/         | /\w+/      | /\w+/      | /\w+/     |
    When executing query, fill replace holders with element index of 0 in job_id:
      """
      STOP JOB {};
      """
    Then the result should be, in order:
      | Result        |
      | "Job stopped" |

# This is skipped becuase it is hard to simulate the situation
# When executing query:
# """
# RECOVER JOB;
# """
# Then the result should be successful

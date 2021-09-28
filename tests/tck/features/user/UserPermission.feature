@mintest
Feature: User & privilege Test

  Scenario: CheckUserJobPermission
    Given create a space with following options:
      | name | admin_job_space |
    When executing query:
      """
      SHOW SPACES
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER job_admin WITH PASSWORD "password"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT ADMIN on admin_job_space to job_admin
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER job_dba WITH PASSWORD "password"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT DBA on admin_job_space to job_dba
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER job_user WITH PASSWORD "password"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT USER on admin_job_space to job_user
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER job_guest WITH PASSWORD "password"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT GUEST on admin_job_space to job_guest
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER job_none WITH PASSWORD "password"
      """
    Then the execution should be successful
    And wait 6 seconds
    When try to execute query on admin_job_space from user root with password nebula:
      """
      SHOW USERS
      """
    Then the result should contain:
      | Account     |
      | "root"      |
      | "job_admin" |
      | "job_dba"   |
      | "job_user"  |
      | "job_guest" |
      | "job_none"  |
    When try to execute query on admin_job_space from user job_none with password password:
      """
      SUBMIT JOB COMPACT;
      """
    Then a PermissionError should be raised at runtime: No permission to read space
    When try to execute query on admin_job_space from user job_none with password password:
      """
      SHOW JOBS
      """
    Then a PermissionError should be raised at runtime: No permission to read space
    When try to execute query on admin_job_space from user job_guest with password password:
      """
      SUBMIT JOB COMPACT;
      """
    Then a PermissionError should be raised at runtime: No permission to write data
    When try to execute query on admin_job_space from user job_guest with password password:
      """
      SHOW JOBS
      """
    Then the result should be, in order:
      | Job Id | Command | Status | Start Time | Stop Time |
    When try to execute query on admin_job_space from user job_user with password password:
      """
      SUBMIT JOB COMPACT;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 1 seconds
    When try to execute query on admin_job_space from user job_user with password password:
      """
      SHOW JOBS
      """
    Then the result should be, in order:
      | Job Id | Command   | Status     | Start Time | Stop Time |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
    When try to execute query on admin_job_space from user job_dba with password password:
      """
      SUBMIT JOB COMPACT;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 1 seconds
    When try to execute query on admin_job_space from user job_dba with password password:
      """
      SHOW JOBS
      """
    Then the result should be, in order:
      | Job Id | Command   | Status     | Start Time | Stop Time |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
    When try to execute query on admin_job_space from user job_admin with password password:
      """
      SUBMIT JOB COMPACT;
      """
    Then the result should be, in any order:
      | New Job Id |
      | /\d+/      |
    And wait 1 seconds
    When try to execute query on admin_job_space from user job_admin with password password:
      """
      SHOW JOBS
      """
    Then the result should be, in order:
      | Job Id | Command   | Status     | Start Time | Stop Time |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |

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
    Then change user to root with password nebula
    When executing query:
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
    Then change user to job_none with password password
    When use current space
    Then a PermissionError should be raised at runtime: No permission to read space
    When executing query:
      """
      SUBMIT JOB COMPACT;
      """
    Then a SemanticError should be raised at runtime: Space was not chosen
    When executing query:
      """
      SHOW JOBS
      """
    Then a SemanticError should be raised at runtime: Space was not chosen
    Then change user to job_guest with password password
    When use current space
    Then the execution should be successful
    When executing query:
      """
      SUBMIT JOB COMPACT;
      """
    Then a PermissionError should be raised at runtime: No permission to write data
    When executing query:
      """
      SHOW JOBS
      """
    Then the result should be, in order:
      | Job Id | Command | Status | Start Time | Stop Time |
    Then change user to job_user with password password
    When use current space
    Then the execution should be successful
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
      SHOW JOBS
      """
    Then the result should be, in order:
      | Job Id | Command   | Status     | Start Time | Stop Time |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
    Then change user to job_dba with password password
    When use current space
    Then the execution should be successful
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
      SHOW JOBS
      """
    Then the result should be, in order:
      | Job Id | Command   | Status     | Start Time | Stop Time |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
    Then change user to job_admin with password password
    When use current space
    Then the execution should be successful
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
      SHOW JOBS
      """
    Then the result should be, in order:
      | Job Id | Command   | Status     | Start Time | Stop Time |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |
      | /\d+/  | "COMPACT" | "FINISHED" | /\w+/      | /\w+/     |

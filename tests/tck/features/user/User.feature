Feature: User & privilege Test

  Background:
    Given an empty graph

  Scenario: Create User
    When executing query:
      """
      DROP USER IF EXISTS user1
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER user1 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      DROP USER IF EXISTS user2
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER user2
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER user1 WITH PASSWORD "pwd1"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW USERS
      """
    Then the result should contain:
      | Account |
      | "root"  |
      | "user1" |
      | "user2" |

  Scenario: Alter user
    When executing query:
      """
      DROP USER IF EXISTS user_tmp
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER USER user_tmp WITH PASSWORD "pwd1"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE USER IF NOT EXISTS user2 WITH PASSWORD "pwd2"
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER USER user2 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      CHANGE PASSWORD user2 FROM "pwd2" TO "pwd1"
      """
    Then a ExecutionError should be raised at runtime: Invalid password!
    When executing query:
      """
      CHANGE PASSWORD user2 FROM "pwd1" TO "pwd2"
      """
    Then the execution should be successful

  Scenario: Drop user
    When executing query:
      """
      CREATE USER IF NOT EXISTS usertmp
      """
    Then the execution should be successful
    When executing query:
      """
      DROP USER usertmp
      """
    Then the execution should be successful
    When executing query:
      """
      DROP USER IF EXISTS usertmp
      """
    Then the execution should be successful
    When executing query:
      """
      DROP USER usertmp
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: Change password of not existing user
    When executing query:
      """
      DROP USER IF EXISTS usertmp
      """
    Then the execution should be successful
    When executing query:
      """
      CHANGE PASSWORD usertmp FROM "pwd" TO "pwd"
      """
    Then a ExecutionError should be raised at runtime:

  @skip
  Scenario: Change password of LDAP authentication, this case should be redesign
    When executing query:
      """
      CHANGE PASSWORD user1 FROM "pwd" TO "pwd"
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: Change password
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      CHANGE PASSWORD user1 FROM "pwd" TO "newpwd1"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      CHANGE PASSWORD user1 FROM "pwd1" TO "newpwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      CHANGE PASSWORD user1 FROM "newpwd1" TO "pwd1"
      """
    Then the execution should be successful

  Scenario: Grant privilege
    When executing query:
      """
      CREATE SPACE IF NOT EXISTS user_tmp_space(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query:
      """
      GRANT DBA TO user1
      """
    Then a SyntaxError should be raised at runtime:
    When executing query:
      """
      DROP USER IF EXISTS usertmp
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT DBA ON user_tmp_space TO usertmp
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE USER IF NOT EXISTS usertmp WITH PASSWORD "usertmp"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT GOD ON user_tmp_space TO usertmp
      """
    Then a PermissionError should be raised at runtime:
    When executing query:
      """
      GRANT DBA ON user_tmp_space TO usertmp
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER IF NOT EXISTS usertmp_2 WITH PASSWORD "usertmp_2"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT GUEST ON user_tmp_space TO usertmp_2
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ROLES IN user_tmp_space
      """
    Then the result should contain:
      | Account     | Role Type |
      | "usertmp"   | "DBA"     |
      | "usertmp_2" | "GUEST"   |
    When executing query:
      """
      DROP SPACE user_tmp_space;
      """
    Then the execution should be successful

  Scenario: Grant privilege on not existing space
    When executing query:
      """
      DROP SPACE IF EXISTS user_tmp_space_2
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE DBA ON user_tmp_space_2 TO user1
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: Revoke role
    When executing query:
      """
      CREATE SPACE IF NOT EXISTS user_tmp_space_3(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE DBA ON user_tmp_space_3 TO user1
      """
    Then the execution should be successful
    When executing query:
      """
      REVOKE ROLE ADMIN ON user_tmp_space_3 FROM user1
      """
    Then a ExecutionError should be raised at runtime: Improper role!
    When executing query:
      """
      REVOKE ROLE DBA ON user_tmp_space_3 FROM user1
      """
    Then the execution should be successful
    When executing query:
      """
      REVOKE ROLE DBA ON user_tmp_space_3 FROM user1
      """
    Then a ExecutionError should be raised at runtime: Role not existed!
    When executing query:
      """
      DROP SPACE IF EXISTS revoke_tmp_space
      """
    Then the execution should be successful
    When executing query:
      """
      REVOKE ROLE DBA ON revoke_tmp_space FROM usertmp
      """
    Then a ExecutionError should be raised at runtime: SpaceNotFound
    When executing query:
      """
      DROP USER IF EXISTS user_revoke_tmp
      """
    Then the execution should be successful
    When executing query:
      """
      REVOKE ROLE DBA ON user_tmp_space_3 FROM user_revoke_tmp
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      DROP SPACE user_tmp_space_3;
      """
    Then the execution should be successful

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
      DROP USER user1
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      CREATE USER user1 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    When executing query:
      """
      DESC USER user1;
      """
    Then the result should be, in any order:
      | role | space |
    And wait 6 seconds
    When verify login with user "user2" and password "pwd1"
    When executing query:
      """
      DROP USER IF EXISTS user2
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER IF NOT EXISTS user2
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER IF NOT EXISTS user2
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER user2
      """
    Then a ExecutionError should be raised at runtime:
    And wait 6 seconds
    When verify login with user "user2"
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
    When executing query:
      """
      CREATE USER u
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER u123456789ABCDEF
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER u123456789ABCDEFG
      """
    Then a SemanticError should be raised at runtime: Username exceed maximum length 16 characters.
    When executing query:
      """
      CREATE USER 123456
      """
    Then a SyntaxError should be raised at runtime: syntax error near `123456'
    When executing query:
      """
      CREATE USER u&b
      """
    Then a SyntaxError should be raised at runtime: syntax error near `&b'
    When executing query:
      """
      CREATE USER `用户A`
      """
    Then the execution should be successful
    And wait 6 seconds
    When verify login with user "用户A"
    When executing query:
      """
      CREATE USER A1;
      CREATE USER a1;
      """
    Then the execution should be successful
    And wait 6 seconds
    When verify login with user "A1"
    When verify login with user "a1"
    When executing query:
      """
      CREATE USER `CREATE`;
      CREATE USER `ROLE`;
      """
    Then the execution should be successful
    When verify login with user "CREATE"
    When verify login with user "ROLE"
    When executing query:
      """
      CREATE USER u3 WITH PASSWORD "012345678910111213141516";
      CREATE USER u4 WITH PASSWORD "0";
      """
    Then the execution should be successful
    And wait 6 seconds
    When verify login with user "u3" and password "012345678910111213141516"
    When verify login with user "u4" and password "0"
    When executing query:
      """
      CREATE USER u5 WITH PASSWORD "0123456789101112131415161";
      """
    Then a SemanticError should be raised at runtime: Password exceed maximum length 24 characters.
    When executing query:
      """
      CREATE USER u6 WITH PASSWORD "中文密码^*()12";
      """
    Then the execution should be successful
    And wait 6 seconds
    When verify login with user "u6" and password "中文密码^*()12"
    When executing query:
      """
      DROP USER IF EXISTS u6;
      """
    Then the execution should be successful
    # TODO(shylock) fix it
    # When executing query:
    # """
    # DESC USER u6;
    # """
    # Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      DROP USER IF EXISTS u6;
      """
    Then the execution should be successful
    When executing query:
      """
      DROP USER u6;
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      DROP USER root;
      """
    Then a SemanticError should be raised at runtime: Can't drop root user.

  Scenario: User roles
    When executing query:
      """
      CREATE USER user_mlt_roles;
      GRANT ROLE USER ON nba TO user_mlt_roles;
      GRANT ROLE GUEST ON student TO user_mlt_roles;
      """
    Then the execution should be successful
    When executing query:
      """
      DESC USER user_mlt_roles;
      """
    Then the result should be, in any order:
      | role    | space     |
      | "USER"  | "nba"     |
      | "GUEST" | "student" |
    When executing query:
      """
      DROP USER user_mlt_roles;
      """
    Then the execution should be successful
    # TODO(shylock) fix me
    # When executing query:
    # """
    # DESC USER user_mlt_roles
    # """
    # Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      CREATE USER user_mlt_roles;
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE ADMIN ON nba TO user_mlt_roles;
      GRANT ROLE ADMIN ON student TO user_mlt_roles;
      GRANT ROLE GUEST ON nba_int_vid TO user_mlt_roles;
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE DBA ON nba TO user_mlt_roles;
      """
    Then the execution should be successful
    When executing query:
      """
      DESC USER user_mlt_roles;
      """
    Then the result should be, in any order:
      | role    | space         |
      | "DBA"   | "nba"         |
      | "ADMIN" | "student"     |
      | "GUEST" | "nba_int_vid" |
    When executing query:
      """
      GRANT ROLE ADMIN ON nba TO user_mlt_roles;
      GRANT ROLE GUEST ON nba TO user_mlt_roles;
      GRANT ROLE USER ON nba TO user_mlt_roles;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ROLES IN nba
      """
    Then the result should be, in any order:
      | Account           | Role Type |
      | "test_permission" | "USER"    |
      | "user_mlt_roles"  | "USER"    |
    When executing query:
      """
      DESC USER user_mlt_roles;
      """
    Then the result should be, in any order:
      | role    | space         |
      | "USER"  | "nba"         |
      | "ADMIN" | "student"     |
      | "GUEST" | "nba_int_vid" |
    When executing query:
      """
      GRANT ROLE ADMIN ON not_exists TO user_mlt_roles;
      """
    Then a ExecutionError should be raised at runtime: SpaceNotFound: SpaceName `not_exists`
    When executing query:
      """
      GRANT ROLE ADMIN ON nba TO not_exists;
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      GRANT not_exists ADMIN ON nba TO user_mlt_roles;
      """
    Then a SyntaxError should be raised at runtime: syntax error near `not_exists'
    When executing query:
      """
      GRANT GOD ON nba TO user_mlt_roles;
      """
    Then a PermissionError should be raised at runtime: No permission to grant/revoke god user.
    When executing query:
      """
      REVOKE ROLE USER ON nba FROM user_mlt_roles;
      """
    Then the execution should be successful
    When executing query:
      """
      DESC USER user_mlt_roles;
      """
    Then the result should be, in any order:
      | role    | space         |
      | "ADMIN" | "student"     |
      | "GUEST" | "nba_int_vid" |
    When executing query:
      """
      REVOKE ROLE GUEST ON student FROM user_mlt_roles;
      """
    Then a ExecutionError should be raised at runtime: Improper role!
    When executing query:
      """
      REVOKE ROLE ADMIN ON nba FROM user_mlt_roles;
      """
    Then a ExecutionError should be raised at runtime: Role not existed!
    When executing query:
      """
      REVOKE ROLE not_exists ON nba FROM user_mlt_roles;
      """
    Then a SyntaxError should be raised at runtime: syntax error near `not_exists'
    When executing query:
      """
      REVOKE ROLE GOD ON nba FROM root;
      """
    Then a PermissionError should be raised at runtime: Permission denied
    When executing query:
      """
      REVOKE ROLE USER ON not_exists FROM user_mlt_roles;
      """
    Then a ExecutionError should be raised at runtime: SpaceNotFound: SpaceName `not_exists`
    When executing query:
      """
      REVOKE ROLE USER ON nba FROM not_exists;
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      DROP USER user_mlt_roles
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ROLES IN nba
      """
    Then the result should be, in any order:
      | Account           | Role Type |
      | "test_permission" | "USER"    |
    When executing query:
      """
      GRANT ROLE ADMIN ON nba TO user_mlt_roles;
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      REVOKE ROLE ADMIN ON nba FROM user_mlt_roles;
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      GRANT GUEST ON nba TO root;
      """
    Then a SemanticError should be raised at runtime: User 'root' is GOD, cannot be granted.
    When executing query:
      """
      SHOW ROLES IN not_exists
      """
    Then a ExecutionError should be raised at runtime: SpaceNotFound: SpaceName `not_exists`

  Scenario: Recreate space roles
    When executing query:
      """
      CREATE SPACE test_roles(partition_num=1, replica_factor=1, vid_type=int64);
      CREATE USER IF NOT EXISTS user_roles WITH PASSWORD "pwd";
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      GRANT ROLE ADMIN ON test_roles TO user_roles;
      """
    Then the execution should be successful
    When executing query:
      """
      DROP SPACE test_roles;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE SPACE test_roles(partition_num=1, replica_factor=1, vid_type=int64);
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      SHOW ROLES IN test_roles
      """
    Then the result should be, in any order:
      | Account | Role Type |
    When executing query:
      """
      DROP SPACE test_roles
      """
    Then the execution should be successful

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
    And wait 6 seconds
    When verify login with user "user2" and password "pwd1"
    When executing query:
      """
      CHANGE PASSWORD user2 FROM "pwd2" TO "pwd1"
      """
    Then a ExecutionError should be raised at runtime: Invalid password!
    When executing query:
      """
      CHANGE PASSWORD user2 FROM "pwd1" TO "01234567890111213141516171"
      """
    Then a SemanticError should be raised at runtime: New password exceed maximum length 24 characters.
    When executing query:
      """
      CHANGE PASSWORD user2 FROM "pwd1" TO "pwd2"
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER IF NOT EXISTS u7
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER USER u7 WITH PASSWORD "pwd1"
      """
    Then the execution should be successful
    And wait 6 seconds
    When verify login with user "u7" and password "pwd1"
    When executing query:
      """
      ALTER USER u7 WITH PASSWORD "0123456789011121314151617"
      """
    Then a SemanticError should be raised at runtime: Password exceed maximum length 24 characters.
    When executing query:
      """
      DROP USER IF EXISTS u7
      """
    Then the execution should be successful
    When executing query:
      """
      CHANGE PASSWORD u7 FROM "pwd1" TO "nebula"
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      ALTER USER not_exists WITH PASSWORD "pwd1"
      """
    Then a ExecutionError should be raised at runtime: User not existed!
    When executing query:
      """
      CHANGE PASSWORD root FROM "nebula" TO "root"
      """
    Then the execution should be successful
    When executing query:
      """
      CHANGE PASSWORD root FROM "root" TO "nebula"
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER IF NOT EXISTS u8
      """
    Then the execution should be successful
    When executing query:
      """
      CHANGE PASSWORD u8 FROM "" TO "pwd2"
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

  Scenario: Describe User
    When executing query:
      """
      CREATE SPACE IF NOT EXISTS user_tmp_space_4(partition_num=1, replica_factor=1, vid_type=FIXED_STRING(8))
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
      GRANT ROLE ADMIN ON user_tmp_space_4 TO user1
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE USER IF NOT EXISTS user2 WITH PASSWORD "pwd2"
      """
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE ADMIN ON user_tmp_space_4 TO user2
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
    When executing query:
      """
      DESC USER root
      """
    Then the result should be, in any order, with relax comparison:
      | role  | space |
      | "GOD" | ""    |
    When executing query:
      """
      DESC USER user1
      """
    Then the result should be, in any order, with relax comparison:
      | role    | space              |
      | "ADMIN" | "user_tmp_space_4" |
    When executing query:
      """
      DESC USER user1 | YIELD $-.space as sp
      """
    Then the result should be, in any order, with relax comparison:
      | sp                 |
      | "user_tmp_space_4" |
    When executing query:
      """
      DESC USER user_not_exist
      """
    Then the result should be, in any order, with relax comparison:
      | role | space |
    When executing query with user "user1" and password "pwd1":
      """
      DESC USER user1
      """
    Then the result should be, in any order, with relax comparison:
      | role    | space              |
      | "ADMIN" | "user_tmp_space_4" |
    When executing query:
      """
      REVOKE ROLE ADMIN ON user_tmp_space_4 FROM user1
      """
    Then the execution should be successful
    When executing query:
      """
      DESC USER user1
      """
    Then the result should be, in any order:
      | role | space |
    When executing query with user "user1" and password "pwd1":
      """
      DESC USER user2
      """
    Then a PermissionError should be raised at runtime:
    When executing query:
      """
      GRANT ROLE GUEST ON user_tmp_space_4 TO user1
      """
    Then the execution should be successful
    When executing query with user "user1" and password "pwd1":
      """
      DESC USER root
      """
    Then a PermissionError should be raised at runtime:

# TODO(shylock) fix it
# When executing query:
# """
# DESCRIBE USER not_exists
# """
# Then a ExecutionError should be raised at runtime: User not existed!

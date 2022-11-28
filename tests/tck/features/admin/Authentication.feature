# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test Authentication

  Background:
    Given a nebulacluster with 1 graphd and 1 metad and 1 storaged and 0 listener:
      """
      graphd:failed_login_attempts=5
      graphd:password_lock_time_in_secs=5
      """

  @distonly
  Scenario: Test login with invalid password
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD 'nebula1';
      CREATE SPACE IF NOT EXISTS root_space(vid_type=int);
      USE root_space;
      """
    Then the execution should be successful
    And wait 3 seconds
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 4
      """
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 3
      """
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 2
      """
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 1
      """
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      5 times consecutive incorrect passwords has been input, user name: user1 has been locked, try again in 5 seconds
      """
    # Login with the correct password when the user is locked should fail
    When login "graphd[0]" with "user1" and "nebula1" should fail:
      """
      5 times consecutive incorrect passwords has been input, user name: user1 has been locked
      """
    # Wail the account to be unlocked
    Then wait 6 seconds
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 4
      """
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 3
      """
    # A successful login should resert the remaining password attempts
    When login "graphd[0]" with "user1" and "nebula1"
    Then the execution should be successful
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 4
      """

  @distonly
  Scenario: Test login with invalid password multi users
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD 'nebula1';
      CREATE USER IF NOT EXISTS user2 WITH PASSWORD 'nebula2';
      CREATE SPACE IF NOT EXISTS root_space(vid_type=int);
      USE root_space;
      """
    Then the execution should be successful
    And wait 3 seconds
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 4
      """
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 3
      """
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 2
      """
    # User2 login
    When login "graphd[0]" with "user2" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 4
      """
    When login "graphd[0]" with "user2" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 3
      """
    # User1 login
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 1
      """
    # User1 lock
    When login "graphd[0]" with "user1" and "wrongPassword" should fail:
      """
      5 times consecutive incorrect passwords has been input, user name: user1 has been locked, try again in 5 seconds
      """
    Then wait 6 seconds
    When login "graphd[0]" with "user2" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 2
      """
    When login "graphd[0]" with "user2" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 1
      """
    When login "graphd[0]" with "user2" and "nebula2"
    Then the execution should be successful
    When login "graphd[0]" with "user1" and "nebula1"
    Then the execution should be successful

  @distonly
  Scenario: God can't be granted
    When executing query:
      """
      GRANT ROLE User ON test TO root
      """
    Then a SemanticError should be raised at runtime: User 'root' is GOD, cannot be granted.

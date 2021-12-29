# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test Authentication

  Background:
    Given a nebulacluster with 3 graphd and 1 metad and 1 storaged

  Scenario: Test login with invalid password
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD 'nebula1';
      CREATE USER IF NOT EXISTS user2 WITH PASSWORD 'nebula2';
      CREATE SPACE IF NOT EXISTS root_space(vid_type=int);
      USE root_space;
      """
    Then the execution should be successful
    And wait 3 seconds
    When login "graphd[1]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 4
      """
    When login "graphd[1]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 3
      """
    When login "graphd[1]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 2
      """
    When login "graphd[1]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 1
      """
    When login "graphd[1]" with "user1" and "wrongPassword" should fail:
      """
      5 times consecutive incorrect passwords has been input, user name: user2 has been blocked, try again in 10 seconds
      """
    Then wait 11 seconds
    When login "graphd[1]" with "user1" and "wrongPassword" should fail:
      """
      Invalid password, remaining attempts: 4
      """

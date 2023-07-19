# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
@skip
Feature: Example

  Scenario: test with disable authorize
    Given a nebulacluster with 1 graphd and 1 metad and 1 storaged and 0 listener:
      """
      graphd:enable_authorize=false
      """
    When executing query:
      """
      CREATE USER user1 WITH PASSWORD 'nebula';
      CREATE SPACE s1(vid_type=int)
      """
    And wait 3 seconds
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE god on s1 to user1
      """
    Then an PermissionError should be raised at runtime: No permission to grant/revoke god user.

  Scenario: test with enable authorize
    Given a nebulacluster with 1 graphd and 1 metad and 1 storaged and 0 listener:
      """
      graphd:enable_authorize=true
      """
    When executing query:
      """
      CREATE USER user1 WITH PASSWORD 'nebula';
      CREATE SPACE s1(vid_type=int)
      """
    And wait 3 seconds
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE god on s1 to user1
      """
    Then an PermissionError should be raised at runtime: No permission to grant/revoke god user.

  Scenario: test with auth type is cloud
    Given a nebulacluster with 1 graphd and 1 metad and 1 storaged and 0 listener:
      """
      graphd:auth_type=cloud
      """
    When executing query:
      """
      CREATE USER user1 WITH PASSWORD 'nebula';
      CREATE SPACE s1(vid_type=int)
      """
    And wait 3 seconds
    Then the execution should be successful
    When executing query:
      """
      GRANT ROLE god on s1 to user1
      """
    Then an PermissionError should be raised at runtime: Cloud authenticate user can't write role.

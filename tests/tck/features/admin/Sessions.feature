# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test sessions

  Background:
    Given a nebulacluster with 3 graphd and 1 metad and 1 storaged and 0 listener

  @distonly
  Scenario: Show sessions
    When executing query:
      """
      SHOW SESSIONS;
      """
    Then the result should contain:
      | SessionId | UserName | SpaceName | CreateTime | UpdateTime | GraphAddr | Timezone | ClientIp           |
      | /\d+/     | "root"   | ""        | /.*/       | /.*/       | /.*/      | 0        | /\b127\.0\.0\.1\b/ |
    When executing query:
      """
      CREATE USER user1 WITH PASSWORD 'nebula1';
      CREATE SPACE IF NOT EXISTS s1(vid_type=int);
      USE s1;
      """
    Then the execution should be successful
    And wait 3 seconds
    When login "graphd[1]" with "user1" and "nebula1"
    And executing query:
      """
      SHOW SESSIONS;
      """
    Then the result should contain, replace the holders with cluster info:
      | SessionId | UserName | SpaceName | CreateTime | UpdateTime | GraphAddr                                           | Timezone | ClientIp           |
      | /\d+/     | "root"   | "s1"      | /.*/       | /.*/       | "127.0.0.1:${cluster.graphd_processes[0].tcp_port}" | 0        | /\b127\.0\.0\.1\b/ |
      | /\d+/     | "user1"  | ""        | /.*/       | /.*/       | "127.0.0.1:${cluster.graphd_processes[1].tcp_port}" | 0        | /\b127\.0\.0\.1\b/ |

  @distonly
  Scenario: Show local sessions
    When executing query:
      """
      SHOW SESSIONS;
      """
    Then the result should contain:
      | SessionId | UserName | SpaceName | CreateTime | UpdateTime | GraphAddr | Timezone | ClientIp           |
      | /\d+/     | "root"   | ""        | /.*/       | /.*/       | /.*/      | 0        | /\b127\.0\.0\.1\b/ |
    When executing query:
      """
      CREATE USER IF NOT EXISTS user1 WITH PASSWORD 'nebula1';
      CREATE USER IF NOT EXISTS user2 WITH PASSWORD 'nebula2';
      CREATE SPACE IF NOT EXISTS root_space(vid_type=int);
      USE root_space;
      """
    Then the execution should be successful
    And wait 3 seconds
    When login "graphd[1]" with "user1" and "nebula1"
    Then the execution should be successful
    When login "graphd[2]" with "user2" and "nebula2"
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      SHOW SESSIONS;
      """
    Then the result should contain, replace the holders with cluster info:
      | SessionId | UserName | SpaceName    | CreateTime | UpdateTime | GraphAddr                                           | Timezone | ClientIp           |
      | /\d+/     | "root"   | "root_space" | /.*/       | /.*/       | "127.0.0.1:${cluster.graphd_processes[0].tcp_port}" | 0        | /\b127\.0\.0\.1\b/ |
      | /\d+/     | "user1"  | ""           | /.*/       | /.*/       | "127.0.0.1:${cluster.graphd_processes[1].tcp_port}" | 0        | /\b127\.0\.0\.1\b/ |
      | /\d+/     | "user2"  | ""           | /.*/       | /.*/       | "127.0.0.1:${cluster.graphd_processes[2].tcp_port}" | 0        | /\b127\.0\.0\.1\b/ |
    When executing query:
      """
      SHOW LOCAL SESSIONS;
      """
    Then the result should contain, replace the holders with cluster info:
      | SessionId | UserName | SpaceName    | CreateTime | UpdateTime | GraphAddr                                           | Timezone | ClientIp           |
      | /\d+/     | "root"   | "root_space" | /.*/       | /.*/       | "127.0.0.1:${cluster.graphd_processes[0].tcp_port}" | 0        | /\b127\.0\.0\.1\b/ |

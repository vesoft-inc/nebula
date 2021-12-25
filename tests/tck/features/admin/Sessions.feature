# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test sessions

  Background:
    Given a nebulacluster with 3 graphd and 1 metad and 1 storaged

  Scenario: Show sessions
    When executing query:
      """
      SHOW SESSIONS;
      """
    Then the result should contain:
      | SessionId | UserName | SpaceName | CreateTime | UpdateTime | GraphAddr | Timezone | ClientIp         |
      | /\d+/     | "root"   | ""        | /.*/       | /.*/       | /.*/      | 0        | /.*(127.0.0.1)$/ |
    When executing query:
      """
      CREATE USER user1 WITH PASSWORD 'nebula1';
      CREATE SPACE s1(vid_type=int);
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
      | SessionId | UserName | SpaceName | CreateTime | UpdateTime | GraphAddr                                           | Timezone | ClientIp         |
      | /\d+/     | "root"   | "s1"      | /.*/       | /.*/       | "127.0.0.1:${cluster.graphd_processes[0].tcp_port}" | 0        | /.*(127.0.0.1)$/ |
      | /\d+/     | "user1"  | ""        | /.*/       | /.*/       | "127.0.0.1:${cluster.graphd_processes[1].tcp_port}" | 0        | /.*(127.0.0.1)$/ |

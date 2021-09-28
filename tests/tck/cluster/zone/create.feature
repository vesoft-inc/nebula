# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Zone create

  Scenario: create zone and space
    Given a nebulacluster with 1 graphd and 1 metad and 6 storaged
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should be, in order:
      | Host                                             | Port  | Status   | Leader count | Leader distribution  | Partition distribution |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "Total"                                          | EMPTY | EMPTY    | 0            | EMPTY                | EMPTY                  |
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z2 "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-3.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z3 "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should be, in any order:
      | Name | Host                                                          | Port |
      | "z1" | /.*-storaged-0.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z1" | /.*-storaged-1.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z2" | /.*-storaged-2.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z2" | /.*-storaged-3.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z3" | /.*-storaged-4.*-storaged-headless.default.svc.cluster.local/ | 9779 |
      | "z3" | /.*-storaged-5.*-storaged-headless.default.svc.cluster.local/ | 9779 |
    When executing query:
      """
      ADD GROUP g1 z1,z2,z3;
      ADD GROUP g2 z1,z2;
      ADD GROUP g3 z1;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW GROUPS
      """
    Then the result should be, in order:
      | Name | Zone |
      | "g1" | "z1" |
      | "g1" | "z2" |
      | "g1" | "z3" |
      | "g2" | "z1" |
      | "g2" | "z2" |
      | "g3" | "z1" |
    When executing query:
      """
      CREATE SPACE s1 (vid_type=int, replica_factor=3, partition_num=4) on g1
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE SPACE s2 (vid_type=int, replica_factor=3, partition_num=4) on g2
      """
    Then an ExecutionError should be raised at runtime: Invalid parm!
    When executing query:
      """
      CREATE SPACE s2 (vid_type=int, replica_factor=1, partition_num=4) on g2
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE SPACE s4 (vid_type=int, replica_factor=1, partition_num=4) on g4
      """
    Then an ExecutionError should be raised at runtime: Group not existed!
    Then drop the used nebulacluster

  Scenario: create zone and space nagative
    Given a nebulacluster with 1 graphd and 1 metad and 3 storaged
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should be, in order:
      | Host                                             | Port  | Status   | Leader count | Leader distribution  | Partition distribution |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | /.*-storaged-headless.default.svc.cluster.local/ | 9779  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "Total"                                          | EMPTY | EMPTY    | 0            | EMPTY                | EMPTY                  |
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9889
      """
    Then an ExecutionError should be raised at runtime: Invalid parm!
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9889,
      """
    Then an ExecutionError should be raised at runtime: Invalid parm!
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 127.0.0.1
      """
    Then a SyntaxError should be raised at runtime: syntax error near `127.0.0.1'
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z2 "{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779;
      """
    Then the execution should be successful
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z3 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779
      """
    Then an ExecutionError should be raised at runtime: Invalid parm!
    When executing query:
      """
      ADD GROUP g1 z0;
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!
    When executing query:
      """
      ADD GROUP g1 z1;
      """
    Then the execution should be successful
    When executing query:
      """
      ADD GROUP g2 z1,z0;
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!
    When executing query:
      """
      ADD GROUP g2 z1;
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When executing query:
      """
      ADD GROUP g1 z1,z2;
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When executing query:
      """
      DROP GROUP g1
      """
    Then the execution should be successful
    When executing query:
      """
      ADD GROUP g3 z1;
      """
    Then the execution should be successful
    When executing query:
      """
      ADD GROUP g2 z1,z2;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE SPACE test_2(partition_num=1, replica_factor=1, vid_type=int64) ON g_2
      """
    Then an ExecutionError should be raised at runtime: Group not existed!
    When executing query:
      """
      CREATE SPACE test_3(partition_num=1, replica_factor=3, vid_type=int64) ON g2
      """
    Then an ExecutionError should be raised at runtime: Invalid parm!
    Then drop the used nebulacluster

  Scenario: create group nagative
    Given a nebulacluster with 1 graphd and 1 metad and 6 storaged
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z2 "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-3.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z3 "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779;
      """
    Then the execution should be successful
    # add group with empty zone
    # TODO should verify the error message.
    When executing query, fill replace holders with cluster_name:
      """
      DROP HOST "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779 FROM ZONE z1;
      DROP HOST "{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779 FROM ZONE z1;
      """
    Then the execution should be successful
    When executing query:
      """
      ADD GROUP g1 z1,z2;
      """
    Then an ExecutionError should be raised at runtime: Invalid parm!

  Scenario: create spaces and check parts
    Given a nebulacluster with 1 graphd and 1 metad and 7 storaged
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z2 "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-3.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z3 "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z4 "{}-storaged-6.{}-storaged-headless.default.svc.cluster.local":9779
      """
    Then the execution should be successful
    When executing query:
      """
      ADD GROUP g1 z1;
      ADD GROUP g2 z1,z2,z3;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE SPACE s1 (vid_type=int, replica_factor=1, partition_num=2) on g1;
      CREATE SPACE s2 (vid_type=int, replica_factor=3, partition_num=4) on g2;
      """
    # wait for creating space and electing the leader
    And wait 3 seconds
    Then the execution should be successful
    # should check partitions
    And verify the space partition, space is s1
    And verify the space partition, space is s2
    Then drop the used nebulacluster

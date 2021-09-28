# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Zone drop from

  Scenario: drop host from zone
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
      ADD ZONE z2 "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z3 "{}-storaged-3.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z4 "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779;
      """
    Then the execution should be successful
    When executing query:
      """
      ADD GROUP g1 z1;
      ADD GROUP g2 z2,z3,z4;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE SPACE s2 (vid_type=int, replica_factor=3, partition_num=4) on g2
      """
    Then the execution should be successful
    And wait 2 seconds
    # host no partitions could be dropped
    When executing query, fill replace holders with cluster_name:
      """
      DROP HOST "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779 from zone z1;
      """
    Then the execution should be successful
    When executing query, fill replace holders with cluster_name:
      """
      DROP HOST "{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779 from zone z2;
      """
    Then an ExecutionError should be raised at runtime: Key not existed!
    # a host with partition in default group, could be dropped from zone
    When executing query, fill replace holders with cluster_name:
      """
      CREATE SPACE s0 (vid_type=int, replica_factor=3, partition_num=10)
      """
    Then the execution should be successful
    When executing query, fill replace holders with cluster_name:
      """
      DROP HOST "{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779 from zone z1;
      """
    Then the execution should be successful
    # host has partitions could not be dropped
    When executing query, fill replace holders with cluster_name:
      """
      DROP HOST "{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779 from zone z4;
      """
    Then an ExecutionError should be raised at runtime: Conflict!
    # drop space then drop host
    When executing query:
      """
      DROP SPACE s2
      """
    Then the execution should be successful
    When executing query, fill replace holders with cluster_name:
      """
      DROP HOST "{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779 from zone z4;
      """
    Then the execution should be successful
    Then drop the used nebulacluster

  Scenario: drop zone from group
    Given a nebulacluster with 1 graphd and 1 metad and 6 storaged
    When executing query, fill replace holders with cluster_name:
      """
      ADD ZONE z1 "{}-storaged-0.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-1.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z2 "{}-storaged-2.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z3 "{}-storaged-3.{}-storaged-headless.default.svc.cluster.local":9779;
      ADD ZONE z4 "{}-storaged-4.{}-storaged-headless.default.svc.cluster.local":9779,"{}-storaged-5.{}-storaged-headless.default.svc.cluster.local":9779;
      """
    Then the execution should be successful
    When executing query:
      """
      ADD GROUP g1 z1;
      ADD GROUP g2 z2,z3,z4;
      ADD GROUP g3 z1,z2,z3;
      """
    Then the execution should be successful
    # drop zone, then create group without any zones
    When executing query:
      """
      DROP ZONE z1 FROM GROUP g1
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE SPACE s1 (vid_type=int, replica_factor=1, partition_num=4) ON g1
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!
    When executing query:
      """
      DROP ZONE z1 FROM GROUP g1
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!
    # drop zone without any part
    When executing query:
      """
      DROP ZONE z1 FROM GROUP g3
      """
    Then the execution should be successful
    # drop zone with parts
    When executing query:
      """
      CREATE SPACE s2 (vid_type=int, replica_factor=3, partition_num=4) ON g2
      """
    And wait 2 seconds
    Then the execution should be successful
    When executing query:
      """
      DROP ZONE z2 FROM GROUP g2
      """
    Then an ExecutionError should be raised at runtime: Conflict!
    # drop the zone with other group parts
    When executing query:
      """
      DROP ZONE z2 FROM GROUP g3
      """
    Then the execution should be successful
    # drop the zone with default group parts
    When executing query:
      """
      CREATE SPACE s0 (vid_type=int, replica_factor=3, partition_num=4)
      """
    And wait 2 seconds
    Then the execution should be successful
    When executing query:
      """
      DROP ZONE z3 FROM GROUP g3
      """
    Then the execution should be successful
    Then drop the used nebulacluster

# TODO, more complex cases, e.g. balance, make zone without any part -> drop zone from group

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Merge zone

  Scenario: merge zone
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "test_zone";
      ADD HOSTS "invalid":9779 INTO NEW ZONE "invalid_zone"
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name           | Host        | Port                                      |
      | "test_zone"    | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
      | "default_zone" | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} |
      | "invalid_zone" | "invalid"   | 9779                                      |
    And wait 8 seconds
    When executing query:
      """
      MERGE ZONE "test_zone" INTO "merged_zone"
      """
    Then an ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      MERGE ZONE "test_zone","not_exist" INTO "merged_zone"
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!
    When executing query:
      """
      MERGE ZONE "test_zone","invalid_zone" INTO "default_zone"
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name           | Host        | Port                                      |
      | "default_zone" | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
      | "default_zone" | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} |
      | "default_zone" | "invalid"   | 9779                                      |
    And the result should not contain, replace the holders with cluster info:
      | Name           | Host        | Port                                      |
      | "test_zone"    | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
      | "invalid_zone" | "invalid"   | 9779                                      |
    When executing query:
      """
      DESC ZONES "test_zone"
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!

  Scenario: merge zone part cannot overlap
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And operating process, "start" a new "storaged[4]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO NEW ZONE "z2";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO NEW ZONE "z3";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[4].tcp_port} INTO NEW ZONE "z4";
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query:
      """
      CREATE SPACE test(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z3","z4";
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      USE test;MERGE ZONE "z3","z4" INTO "z5";
      """
    Then an ExecutionError should be raised at runtime: Conflict!
    When executing query:
      """
      USE test;SUBMIT JOB BALANCE ACROSS ZONE REMOVE "z4";
      """
    Then the execution should be successful
    And wait the job to finish
    When executing query:
      """
      MERGE ZONE "z3","z4" INTO "z5";
      """
    Then the execution should be successful
    And verify the space partitions, space is "test"
    When executing query:
      """
      DESC SPACE test;
      """
    Then the result should be, in order:
      | ID | Name   | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones      | Comment |
      | 1  | "test" | 10               | 3              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z1,z2,z5" | EMPTY   |

  Scenario: merge zone in 2 space
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And operating process, "start" a new "storaged[4]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO NEW ZONE "z2";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO NEW ZONE "z3";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[4].tcp_port} INTO NEW ZONE "z4";
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query:
      """
      CREATE SPACE test1(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test2(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z4";
      """
    Then the execution should be successful
    # test2 is not overlap, but test1 is.
    When executing query:
      """
      MERGE ZONE "z1","z2" INTO "z5";
      """
    Then an ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      MERGE ZONE "z3","z4" INTO "z5";
      """
    Then the execution should be successful
    When executing query:
      """
      DESC SPACE test1;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones      | Comment |
      | 1  | "test1" | 10               | 3              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z1,z2,z5" | EMPTY   |
    When executing query:
      """
      DESC SPACE test2;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones      | Comment |
      | 2  | "test2" | 10               | 3              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z1,z2,z5" | EMPTY   |

  Scenario: merge zone in 1 replica space
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO NEW ZONE "z2";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO NEW ZONE "z3";
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query:
      """
      CREATE SPACE test(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      """
    When executing query:
      """
      MERGE ZONE "z1","z2","z3" INTO "z4";
      """
    Then the execution should be successful
    When executing query:
      """
      DESC SPACE test;
      """
    Then the result should be, in order:
      | ID | Name   | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 1  | "test" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |

  Scenario: merge zone in 10+ space
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO NEW ZONE "z2";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO NEW ZONE "z3";
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query:
      """
      CREATE SPACE test1(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test2(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test3(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test4(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test5(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test6(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test7(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test8(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test9(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      CREATE SPACE test10(vid_type=int, replica_factor=1, partition_num=10) on "z1","z2","z3";
      """
    When executing query:
      """
      MERGE ZONE "z1","z2","z3" INTO "z4";
      """
    Then the execution should be successful
    When executing query:
      """
      DESC SPACE test1;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 1  | "test1" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test2;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 2  | "test2" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test3;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 3  | "test3" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test4;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 4  | "test4" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test5;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 5  | "test5" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test6;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 6  | "test6" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test7;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 7  | "test7" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test8;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 8  | "test8" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test9;
      """
    Then the result should be, in order:
      | ID | Name    | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 9  | "test9" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |
    When executing query:
      """
      DESC SPACE test10;
      """
    Then the result should be, in order:
      | ID | Name     | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones | Comment |
      | 10 | "test10" | 10               | 1              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z4"  | EMPTY   |

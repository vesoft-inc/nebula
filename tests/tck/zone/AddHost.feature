# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: add host into zone

  Scenario: Add hosts
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And executing query:
      """
      SHOW HOSTS
      """
    Then the result should contain, replace the holders with cluster info:
      | Host        | Port                                      | Status   | Leader count | Leader distribution  | Partition distribution |
      | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "Total"     | EMPTY                                     | EMPTY    | 0            | EMPTY                | EMPTY                  |
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name           | Host        | Port                                      |
      | "default_zone" | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} |
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port};
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name                         | Host        | Port                                      |
      | /default_zone_127.0.0.1_\d+/ | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
      | "default_zone"               | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} |
    And wait 6 seconds
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should contain, replace the holders with cluster info:
      | Host        | Port                                      | Status   | Leader count | Leader distribution  | Partition distribution |
      | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "Total"     | EMPTY                                     | EMPTY    | 0            | EMPTY                | EMPTY                  |
    When operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS
      127.0.0.1:${cluster.storaged_processes[2].tcp_port},127.0.0.1:${cluster.storaged_processes[3].tcp_port}
      INTO NEW ZONE "test_zone"
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name                         | Host        | Port                                      |
      | /default_zone_127.0.0.1_\d+/ | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
      | "default_zone"               | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} |
      | "test_zone"                  | "127.0.0.1" | ${cluster.storaged_processes[2].tcp_port} |
      | "test_zone"                  | "127.0.0.1" | ${cluster.storaged_processes[3].tcp_port} |
    And wait 8 seconds
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should contain, replace the holders with cluster info:
      | Host        | Port                                      | Status   | Leader count | Leader distribution  | Partition distribution |
      | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[2].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[3].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "Total"     | EMPTY                                     | EMPTY    | 0            | EMPTY                | EMPTY                  |
    When executing query:
      """
      ADD HOSTS "myHost":9779
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name                         | Host        | Port                                      |
      | /default_zone_127.0.0.1_\d+/ | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
      | "default_zone"               | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} |
      | "test_zone"                  | "127.0.0.1" | ${cluster.storaged_processes[2].tcp_port} |
      | "test_zone"                  | "127.0.0.1" | ${cluster.storaged_processes[3].tcp_port} |
      | "default_zone_myHost_9779"   | "myHost"    | 9779                                      |

  Scenario: Add hosts invalid
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When executing query:
      """
      ADD HOSTS 127.0.0.1:test;
      """
    Then a SyntaxError should be raised at runtime: syntax error near `test'
    When executing query:
      """
      ADD HOSTS test:9779;
      """
    Then a SyntaxError should be raised at runtime: syntax error near `test'
    When executing query:
      """
      ADD HOSTS "myHost":9779
      """
    Then the execution should be successful
    When executing query:
      """
      ADD HOSTS "myHost":9779
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When executing query:
      """
      ADD HOSTS "myHost":9779,"abc":9779
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When operating process, "start" a new "storaged[1]":
      """
      local_ip=localhost
      """
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port};
      """
    Then the execution should be successful
    And wait 8 seconds
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should contain, replace the holders with cluster info:
      | Host        | Port                                      | Status   | Leader count | Leader distribution  | Partition distribution |
      | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "Total"     | EMPTY                                     | EMPTY    | 0            | EMPTY                | EMPTY                  |

  Scenario: Add hosts and create space immediately
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And wait 3 seconds
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port};
      CREATE SPACE test(vid_type=int, replica_factor=1, partition_num=10)
      """
    Then an ExecutionError should be raised at runtime: Invalid param!

  Scenario: Drop hosts and readd host
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "test_zone"
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name        | Host        | Port                                      |
      | "test_zone" | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
    When executing query:
      """
      CREATE SPACE test(vid_type=int, replica_factor=1, partition_num=10) on "default_zone"
      """
    And executing query, replace the holders with cluster info:
      """
      DROP HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port}
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should not contain, replace the holders with cluster info:
      | Name        | Host        | Port                                      |
      | "test_zone" | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "test_zone"
      """
    Then the execution should be successful
    When wait 3 seconds
    And executing query:
      """
      SHOW HOSTS
      """
    Then the result should not contain, replace the holders with cluster info:
      | Host        | Port                                      | Status   | Leader count | Leader distribution  | Partition distribution |
      | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |

  Scenario: Add more hosts
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And operating process, "start" a new "storaged[4]"
    And operating process, "start" a new "storaged[5]"
    And operating process, "start" a new "storaged[6]"
    And operating process, "start" a new "storaged[7]"
    And operating process, "start" a new "storaged[8]"
    And operating process, "start" a new "storaged[9]"
    And operating process, "start" a new "storaged[10]"
    And operating process, "start" a new "storaged[11]"
    And operating process, "start" a new "storaged[12]"
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[4].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[5].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[6].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[7].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[8].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[9].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[10].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[11].tcp_port};
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[12].tcp_port};
      """
    Then the execution should be successful
    And wait 12 seconds
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should contain, replace the holders with cluster info:
      | Host        | Port                                       | Status   | Leader count | Leader distribution  | Partition distribution |
      | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[2].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[3].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[4].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[5].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[6].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[7].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[8].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[9].tcp_port}  | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[10].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[11].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "127.0.0.1" | ${cluster.storaged_processes[12].tcp_port} | "ONLINE" | 0            | "No valid partition" | "No valid partition"   |
      | "Total"     | EMPTY                                      | EMPTY    | 0            | EMPTY                | EMPTY                  |

  Scenario: Add hosts and create space
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
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name | Host        | Port                                      |
      | "z1" | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
      | "z2" | "127.0.0.1" | ${cluster.storaged_processes[2].tcp_port} |
      | "z3" | "127.0.0.1" | ${cluster.storaged_processes[3].tcp_port} |
    And wait 8 seconds
    When executing query:
      """
      CREATE SPACE test(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z3"
      """
    Then the execution should be successful
    When wait 3 seconds
    Then verify the space partitions, space is "test"

  Scenario: Add hosts and create space 2
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And operating process, "start" a new "storaged[4]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO NEW ZONE "z2";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[4].tcp_port} INTO NEW ZONE "z3";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name | Host        | Port                                      |
      | "z1" | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
      | "z1" | "127.0.0.1" | ${cluster.storaged_processes[2].tcp_port} |
      | "z2" | "127.0.0.1" | ${cluster.storaged_processes[3].tcp_port} |
      | "z3" | "127.0.0.1" | ${cluster.storaged_processes[4].tcp_port} |
    And wait 8 seconds
    When executing query:
      """
      CREATE SPACE test(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z3"
      """
    Then the execution should be successful
    When wait 3 seconds
    Then verify the space partitions, space is "test"

  Scenario: Add hosts and create space invalid
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
      """
    Then the execution should be successful
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[4].tcp_port} INTO NEW ZONE "z1";
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[4].tcp_port} INTO ZONE "z4";
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!
    When executing query:
      """
      CREATE SPACE invalid_space(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z4"
      """
    Then an ExecutionError should be raised at runtime: Zone not existed!
    When executing query:
      """
      CREATE SPACE invalid_space_2(vid_type=int, replica_factor=4, partition_num=10) on "z1","z2","z3"
      """
    Then an ExecutionError should be raised at runtime: Invalid param!

  Scenario: Add invalid hosts into zone
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
    When executing query:
      """
      ADD HOSTS "invalid":9779 INTO  ZONE "z1";
      ADD HOSTS "invalid_2":9779 INTO NEW ZONE "z4";
      """
    Then the execution should be successful
    When operating process, "stop" "storaged[3]"
    And wait 4 seconds
    When executing query:
      """
      CREATE SPACE space1(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z3"
      """
    And wait 3 seconds
    # zone z3 has no alived hosts
    Then an ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      CREATE SPACE space2(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z3"
      """
    And wait 3 seconds
    # should not create parts in invalid hosts
    Then verify the space partitions, space is "test"
    When executing query:
      """
      CREATE SPACE space3(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z3","z4"
      """
    And wait 3 seconds
    # should not create parts in invalid hosts
    Then verify the space partitions, space is "test"

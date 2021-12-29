# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
@Harris
Feature: add host into zone

  Scenario: Drop hosts
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When executing query, replace the holders with cluster info:
      """
      DROP HOSTS 127.0.0.1:${cluster.storaged_processes[0].tcp_port}
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should be, in order:
      | Name | Host | Port |
    When operating process, "start" a new "storaged[1]"
    When executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port};
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      CREATE SPACE test(vid_type=int, replica_factor=1, partition_num=10)
      """
    Then the execution should be successful
    When executing query, replace the holders with cluster info:
      """
      DROP HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port}
      """
    Then an ExecutionError should be raised at runtime: Conflict!
    When executing query:
      """
      DROP SPACE test;
      """
    When executing query, replace the holders with cluster info:
      """
      DROP HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port}
      """
    Then the execution should be successful
    # Bug here
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should be, in order:
      | Host    | Port  | Status | Leader count | Leader distribution | Partition distribution |
      | "Total" | EMPTY | EMPTY  | 0            | EMPTY               | EMPTY                  |

  Scenario: Drop hosts 2
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When executing query:
      """
      DROP HOSTS "invalid":9779
      """
    Then an ExecutionError should be raised at runtime: No hosts!
    When executing query:
      """
      CREATE SPACE test(vid_type=int, replica_factor=1, partition_num=10)
      """
    Then the execution should be successful
    When executing query, replace the holders with cluster info:
      """
      DROP HOSTS 127.0.0.1:${cluster.storaged_processes[0].tcp_port};
      """
    Then an ExecutionError should be raised at runtime: Conflict!
    When executing query, replace the holders with cluster info:
      """
      DROP SPACE test;
      DROP HOSTS 127.0.0.1:${cluster.storaged_processes[0].tcp_port};
      """
    Then the execution should be successful

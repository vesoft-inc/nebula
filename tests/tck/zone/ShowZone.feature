# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Show zones

  Scenario: show zone
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "test_zone";
      """
    Then the execution should be successful
    And wait 8 seconds
    When executing query:
      """
      SHOW ZONES
      """
    Then the result should contain, replace the holders with cluster info:
      | Name           | Host        | Port                                      |
      | "default_zone" | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} |
      | "test_zone"    | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |
    When executing query:
      """
      SHOW ZONE "default_zone";
      """
    Then the result should contain, replace the holders with cluster info:
      | Hosts       | Port                                      |
      | "127.0.0.1" | ${cluster.storaged_processes[0].tcp_port} |
    And the result should not contain, replace the holders with cluster info:
      | Hosts       | Port                                      |
      | "127.0.0.1" | ${cluster.storaged_processes[1].tcp_port} |

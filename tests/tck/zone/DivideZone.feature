Feature: Divide zone
  Scenario: divide zone
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And operating process, "start" a new "storaged[4]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[4].tcp_port} INTO NEW ZONE "z2";
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query, replace the holders with cluster info:
      """
      DIVIDE zone "z1" into
      "z2" (127.0.0.1:${cluster.storaged_processes[1].tcp_port})
      "z3" (127.0.0.1:${cluster.storaged_processes[2].tcp_port},127.0.0.1:${cluster.storaged_processes[3].tcp_port})
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When executing query, replace the holders with cluster info:
      """
      DIVIDE zone "z1" into
      "z3" (127.0.0.1:${cluster.storaged_processes[0].tcp_port})
      "z4" (127.0.0.1:${cluster.storaged_processes[2].tcp_port},127.0.0.1:${cluster.storaged_processes[3].tcp_port})
      """
    Then an ExecutionError should be raised at runtime: Invalid param!
    When executing query, replace the holders with cluster info:
      """
      DIVIDE zone "z1" into
      "z3" (127.0.0.1:${cluster.storaged_processes[1].tcp_port},127.0.0.1:${cluster.storaged_processes[2].tcp_port})
      "z4" (127.0.0.1:${cluster.storaged_processes[2].tcp_port},127.0.0.1:${cluster.storaged_processes[3].tcp_port})
      """
    Then an ExecutionError should be raised at runtime: Invalid param!
    When executing query, replace the holders with cluster info:
      """
      DIVIDE zone "z1" into
      "z3" (127.0.0.1:${cluster.storaged_processes[1].tcp_port})
      "z3" (127.0.0.1:${cluster.storaged_processes[2].tcp_port},127.0.0.1:${cluster.storaged_processes[3].tcp_port})
      """
    Then an ExecutionError should be raised at runtime: Invalid param!
    When executing query, replace the holders with cluster info:
      """
      DIVIDE zone "z1" into
      "z3" (127.0.0.1:${cluster.storaged_processes[1].tcp_port})
      "z4" (127.0.0.1:${cluster.storaged_processes[2].tcp_port})
      """
    Then an ExecutionError should be raised at runtime: Existed!
    When executing query, replace the holders with cluster info:
      """
      DIVIDE zone "z1" into
      "z3" (127.0.0.1:${cluster.storaged_processes[1].tcp_port})
      "z4" (127.0.0.1:${cluster.storaged_processes[2].tcp_port}) (127.0.0.1:${cluster.storaged_processes[3].tcp_port}
      """
    Then the execution should be successful

  Scenario: divide zone from itself
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO ZONE "z1";
      """
    When executing query, replace the holders with cluster info:
      """
      DIVIDE zone "z1" into
      "z1" (127.0.0.1:${cluster.storaged_processes[1].tcp_port})
      "z2" (127.0.0.1:${cluster.storaged_processes[2].tcp_port}) (127.0.0.1:${cluster.storaged_processes[3].tcp_port}
      """
    Then the execution should be successful

  Scenario: divide zone should update space description
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO NEW ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO ZONE "z1";
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO ZONE "z1";
      """
    When executing query:
      """
      CREATE SPACE test(vid_type=int, replica_factor=1, partition_num=10) on "z1";
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query, replace the holders with cluster info:
      """
      DIVIDE zone "z1" into
      "z3" (127.0.0.1:${cluster.storaged_processes[1].tcp_port})
      "z4" (127.0.0.1:${cluster.storaged_processes[2].tcp_port},127.0.0.1:${cluster.storaged_processes[3].tcp_port})
      """
    Then the execution should be successful
    When executing query:
      """
      DESC SPACE test;
      """
    Then the result should be, in order:
      | ID | Name   | Partition Number | Replica Factor | Charset | Collate    | Vid Type | Atomic Edge | Zones   | Comment |
      | 1  | "test" | 10               | 3              | "utf8"  | "utf8_bin" | "INT64"  | false       | "z3,z4" | EMPTY   |

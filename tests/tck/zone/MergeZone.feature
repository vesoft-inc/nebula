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

	@Harris
  Scenario: merge zone part cannot overlap
    Given a nebulacluster with 1 graphd and 3 metad and 1 storaged
    When operating process, "start" a new "storaged[1]"
    And operating process, "start" a new "storaged[2]"
    And operating process, "start" a new "storaged[3]"
    And operating process, "start" a new "storaged[4]"
    And executing query, replace the holders with cluster info:
      """
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[1].tcp_port} INTO ZONE z1;
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[2].tcp_port} INTO ZONE z2;
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[3].tcp_port} INTO ZONE z3;
      ADD HOSTS 127.0.0.1:${cluster.storaged_processes[4].tcp_port} INTO ZONE z4;
      """
    Then the execution should be successful
		And wait 8 seconds
		When executing query:
		"""
		CREATE SPACE test(vid_type=int, replica_factor=3, partition_num=10) on "z1","z2","z3","z4";
		"""
		Then the execution should be successful
		And wait 3 seconds
		When executing query:
		"""
		USE test;submit job BALANCE zone REMOVE
		"""

	

# 1. from zone 必须存在，to zone 必须不存在
# 2. zone 中的每个 space，合并后，不能有 part 重叠
# 3. merge zone 和 create space 并发？
# 4. merge zone, 1 个 space 3 副本，part 有交集
# 5. merge zone, hosts offline
# 6. merge zone, 2 个 space 的 zone 合并
# 7. merge zone, 100个 space，100 个 part，50 个 zone
# 8. merge 多个 zone，不满足副本数 （特殊情况，on default）
# 9. 单副本，多副本

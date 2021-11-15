# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Insert vertex and edge with if not exists

  Scenario: insert edge with default value
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS V();
      CREATE EDGE IF NOT EXISTS E(rank timestamp default timestamp());
      """
    When try to execute query:
      """
      INSERT VERTEX V() VALUES "v1":()
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT VERTEX V() VALUES "v2":()
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT EDGE E() VALUES "v1"->"v2":()
      """
    Then the execution should be successful
    When executing query:
      """
      (GO FROM "v1" over E yield E.rank union GO FROM "v2" over E REVERSELY yield E.rank) | yield count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 1     |
    And drop the used space

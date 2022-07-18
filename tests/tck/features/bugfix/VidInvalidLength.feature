# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Invalid vid length

  # issue https://github.com/vesoft-inc/nebula/issues/4397
  Scenario: With vertex operate overlength vid with no index
    Given create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(10) |
    When executing query:
      """
      CREATE TAG t1 (col1 int);
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX t1(col1) VALUES "01234567890": (1)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      DELETE VERTEX "01234567890"
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      UPDATE VERTEX "01234567890"
      SET t1.col1 = 1
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      UPSERT VERTEX "01234567890"
      SET t1.col1 = 1
      YIELD $^.t1.col1 AS T1C1
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.

  Scenario: With vertex operate overlength vid with index
    Given create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(10) |
    When executing query:
      """
      CREATE TAG t1 (col1 int);
      CREATE TAG INDEX t1_index ON t1(col1)
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX t1(col1) VALUES "01234567890": (1)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      DELETE VERTEX "01234567890"
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      UPDATE VERTEX "01234567890"
      SET t1.col1 = 1
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      UPSERT VERTEX "01234567890"
      SET t1.col1 = 1
      YIELD $^.t1.col1 AS T1C1
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.

  Scenario: With edge operate overlength vid with no index
    Given create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(10) |
    When executing query:
      """
      CREATE TAG t1 (col1 int);
      CREATE EDGE e(sum int);
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX t1(col1) VALUES "t1": (1);
      INSERT EDGE e(sum) VALUES "t1" -> "01234567890":(3);
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      DELETE EDGE e "t1" -> "01234567890";
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      UPDATE EDGE "t1" -> "01234567890"@0 OF e
      SET sum = 3
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      UPSERT EDGE "t1" -> "01234567890"@0 OF e
      SET sum = 3
      YIELD sum as SUM
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.

  Scenario: With edge operate overlength vid with index
    Given create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(10) |
    When executing query:
      """
      CREATE tag t1 (col1 int);
      CREATE EDGE e(sum int);
      CREATE EDGE INDEX eidx on e(sum);
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX t1(col1) VALUES "t1": (1);
      INSERT edge e(sum) VALUES "t1" -> "01234567890":(3);
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      DELETE EDGE e "t1" -> "01234567890";
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      UPDATE EDGE "t1" -> "01234567890"@0 OF e
      SET sum = 3
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When executing query:
      """
      UPSERT EDGE "t1" -> "01234567890"@0 OF e
      SET sum = 3
      YIELD sum as SUM
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.

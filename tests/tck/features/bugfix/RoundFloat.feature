# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Round the float/double when insert them into integer column

  # issue https://github.com/vesoft-inc/nebula/issues/3473
  Scenario: Insert float/double into a integer column
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      create tag test(a int32);
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT VERTEX test(a) VALUES '101':(3.2);
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX test(a) VALUES '102':(3.8);
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX test(a) VALUES '103':(-3.2);
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX test(a) VALUES '104':(-3.8);
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX test(a) VALUES '104':(2147483647.1);
      """
    Then an ExecutionError should be raised at runtime: Storage Error: Out of range value.
    When executing query:
      """
      FETCH PROP ON test '101', '102', '103', '104' YIELD test.a;
      """
    Then the result should be, in any order, with relax comparison:
      | test.a |
      | 3      |
      | 4      |
      | -3     |
      | -4     |
    Then drop the used space

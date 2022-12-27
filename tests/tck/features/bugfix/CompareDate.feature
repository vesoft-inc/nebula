# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Compare date value

  # #5046
  Scenario: Compare date value
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      create tag date_comp(i1 int, d1 date);
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT VERTEX date_comp(i1, d1) VALUES 'xxx':(1, date());
      """
    Then the execution should be successful
    When try to execute query:
      """
      UPDATE VERTEX ON date_comp 'xxx' SET i1=3 WHEN d1 == date()
      YIELD i1;
      """
    Then the execution should be successful

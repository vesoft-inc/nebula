# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Create duration as default value

  Scenario: Create duration as default value
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      create tag ddl_tag1(col1 DURATION DEFAULT duration({years: 3, months: 2}));
      """
    Then the execution should be successful
    When executing query:
      """
      create edge ddl_edge1(col1 DURATION DEFAULT duration({years: 3, months: 2}));
      """
    Then the execution should be successful

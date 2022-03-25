# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Typo error

  # issue https://github.com/vesoft-inc/nebula/issues/2204
  Scenario: Typo error of KW_VALUE
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      CREATE tag value(value int, values bool)
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG value;
      """
    Then the result should be, in any order:
      | Field    | Type    | Null  | Default | Comment |
      | "value"  | "int64" | "YES" | EMPTY   | EMPTY   |
      | "values" | "bool"  | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      SHOW CREATE TAG value
      """
    Then the result should be, in any order, with relax comparison:
      | Tag     | Create Tag                                                                                          |
      | "value" | 'CREATE TAG `value` (\n `value` int64 NULL,\n `values` bool NULL\n) ttl_duration = 0, ttl_col = ""' |

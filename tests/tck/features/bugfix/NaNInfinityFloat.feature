# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: NaN/Infinity result test

  # issue https://github.com/vesoft-inc/nebula/issues/3473
  Scenario: NaN/Infinity result test
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      Yield 0/0.0
      """
    Then the result should be, in any order, with relax comparison:
      | (0/0) |
      | nan   |
    When executing query:
      """
      Yield 1/0.0
      """
    Then the result should be, in any order, with relax comparison:
      | (1/0) |
      | inf   |
    When executing query:
      """
      Yield -1/0.0
      """
    Then the result should be, in any order, with relax comparison:
      | (-(1)/0) |
      | -inf     |
    When executing query:
      """
      Yield sqrt(-1.0)
      """
    Then the result should be, in any order, with relax comparison:
      | sqrt(-(1)) |
      | nan        |
    Then drop the used space

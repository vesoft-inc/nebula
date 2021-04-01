# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Bound integer insertion test

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG test(id int)
      """

  Scenario Outline: insert bound integer value
    When try to execute query:
      """
      INSERT VERTEX test(id) VALUES '100':(<num>);
      INSERT VERTEX test(id) VALUES '100':(<hex>);
      INSERT VERTEX test(id) VALUES '100':(<oct>);
      """
    Then the execution should be successful
    And drop the used space

    Examples:
      | num                  | hex                 | oct                      |
      | 9223372036854775807  | 0x7fffffffffffffff  | 0777777777777777777777   |
      | 1                    | 0x1                 | 01                       |
      | 0                    | 0x0                 | 00                       |
      | -1                   | -0x1                | -01                      |
      | -9223372036854775808 | -0x8000000000000000 | -01000000000000000000000 |

  Scenario Outline: insert invalid bound integer value
    When try to execute query:
      """
      INSERT VERTEX test(id) VALUES '100':(1);
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX test(id) VALUES '100':(<num>);
      """
    Then a SyntaxError should be raised at compile time: Out of range
    When executing query:
      """
      INSERT VERTEX test(id) VALUES '100':(<hex>);
      """
    Then a SyntaxError should be raised at compile time: Out of range
    When executing query:
      """
      INSERT VERTEX test(id) VALUES '100':(<oct>);
      """
    Then a SyntaxError should be raised at compile time: Out of range
    And drop the used space

    Examples:
      | num                  | hex                 | oct                      |
      | -9223372036854999999 | -0x8000000000036bbf | -01000000000000000665677 |
      | -9223372036854775809 | -0x8000000000000001 | -01000000000000000000001 |
      | 9223372036854775808  | 0x8000000000000000  | 01000000000000000000000  |
      | 9223372036899999999  | 0x8000000002b210ff  | 01000000000000254410377  |

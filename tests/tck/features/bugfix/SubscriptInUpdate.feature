# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test subscript in update

  # #5445
  Scenario: Subscript in update
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      create tag test_tag(i1 int, json string);
      create edge test_edge(i1 int, json string);
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT VERTEX test_tag(i1, json) VALUES 'xxx':(1, '{"a":1,"b":2}');
      """
    Then the execution should be successful
    When try to execute query:
      """
      UPDATE VERTEX ON test_tag 'xxx'
      SET i1=json_extract(json)['b']
      WHEN json_extract(json)['a'] == 1
      YIELD i1;
      """
    Then the result should be, in any order:
      | i1 |
      | 2  |
    When try to execute query:
      """
      INSERT EDGE test_edge(i1, json) VALUES 'xxx'->'xxx':(1, '{"a":1,"b":2}');
      """
    Then the execution should be successful
    When try to execute query:
      """
      UPDATE EDGE ON test_edge 'xxx'->'xxx'
      SET i1=json_extract(json)['b']
      WHEN json_extract(json)['a'] == 1
      YIELD i1;
      """
    Then the result should be, in any order:
      | i1 |
      | 2  |

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Datetime insert mismatched type

  # issue https://github.com/vesoft-inc/nebula-graph/issues/1318
  Scenario: DateTime insert mismatched type
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      create tag ddl_tag1(col1 date default date("2017-03-04"),
                          col2 datetime default datetime("2017-03-04T00:00:01"),
                          col3 time default time("11:11:11"));
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT VERTEX ddl_tag1() VALUES 'test':()
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX ddl_tag1(col1, col2, col3) VALUES 'test':(date("2019-01-02"), date('2019-01-02'), time('11:11:11'))
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX ddl_tag1(col1, col2, col3) VALUES 'test':(datetime("2019-01-02T00:00:00"), datetime('2019-01-02T00:00:00'), time('11:11:11'))
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX ddl_tag1(col1, col2, col3) VALUES 'test':(date("2019-01-02"), datetime('2019-01-02T00:00:00'), datetime('2019-01-02T11:11:11'))
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    Then drop the used space

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: FulltextIndexTest

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
    And add listeners to space

  @ft_index
  Scenario: fulltext ddl
    When executing query:
      """
      CREATE TAG ddl_tag(prop1 string,prop2 fixed_string(20),prop3 int);
      CREATE EDGE ddl_edge(prop1 string,prop2 float);
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      CREATE FULLTEXT TAG INDEX nebula_index_ddl_tag_prop1 on ddl_tag(prop1);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE FULLTEXT TAG INDEX nebula_index_ddl_tag_prop2 on ddl_tag(prop2);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE FULLTEXT TAG INDEX nebula_index_ddl_tag_prop3 on ddl_tag(prop3);
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      SHOW FULLTEXT INDEXES;
      """
    Then the result should be, in any order:
      | Name                         | Schema Type | Schema Name | Fields  | Analyzer  |
      | "nebula_index_ddl_tag_prop1" | "Tag"       | "ddl_tag"   | "prop1" | "default" |
      | "nebula_index_ddl_tag_prop2" | "Tag"       | "ddl_tag"   | "prop2" | "default" |
    When executing query:
      """
      DROP FULLTEXT INDEX nebula_index_ddl_tag_prop1;
      """
    Then the execution should be successful
    When executing query:
      """
      DROP FULLTEXT INDEX nebula_index_ddl_tag_prop2;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW FULLTEXT INDEXES;
      """
    Then the result should be, in any order:
      | Name | Schema Type | Schema Name | Fields | Analyzer |
    When executing query:
      """
      CREATE FULLTEXT TAG INDEX nebula_index_ddl_tag_prop1 on ddl_tag(prop2);
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW FULLTEXT INDEXES;
      """
    Then the result should be, in any order:
      | Name                         | Schema Type | Schema Name | Fields  | Analyzer  |
      | "nebula_index_ddl_tag_prop1" | "Tag"       | "ddl_tag"   | "prop2" | "default" |
    When executing query:
      """
      DROP TAG ddl_tag;
      """
    Then a ExecutionError should be raised at runtime: Related index exists, please drop index first
    When executing query:
      """
      ALTER TAG ddl_tag DROP (prop2);
      """
    Then a ExecutionError should be raised at runtime: Related fulltext index exists, please drop it first
    When executing query:
      """
      ALTER TAG ddl_tag DROP (prop1);
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG ddl_tag ADD (prop1_new string);
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG ddl_tag CHANGE (prop2 string);
      """
    Then a ExecutionError should be raised at runtime: Related fulltext index exists, please drop it first
    When executing query:
      """
      DROP FULLTEXT INDEX nebula_index_ddl_tag_prop1;
      """
    Then the execution should be successful
    When executing query:
      """
      DROP TAG ddl_tag;
      DROP EDGE ddl_edge;
      """
    Then the execution should be successful

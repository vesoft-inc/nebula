# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: FulltextIndexTest_Vid_String

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
    And add listeners to space
    And having executed:
      """
      SIGN IN TEXT SERVICE(elasticsearch:9200, HTTP)
      """

  @ft_index
  Scenario: fulltext demo
    When executing query:
      """
      CREATE TAG ft_tag(prop1 string)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX index_ft_tag_prop1 on ft_tag(prop1)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE FULLTEXT TAG INDEX nebula_index_1 on ft_tag(prop1)
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      INSERT INTO ft_tag(prop1) VALUES "1":("abc");
      """
    Then the execution should be successful
    And wait 5 seconds
    When executed query:
      """
      LOOKUP ON ft_tag where prefix(ft_tag.prop1,"abc")
      YIELD id(vertex) as id, ft_tag.prop1 as prop1
      """
    Then the result should be, in any order:
      | id  | prop1 |
      | "1" | "abc" |

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: FulltextIndexTest_Vid_int

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1     |
      | replica_factor | 1     |
      | vid_type       | INT64 |
    And add listeners to space

  @ft_index
  Scenario: fulltext demo2
    When executing query:
      """
      CREATE TAG ft_tag(prop1 string)
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      CREATE FULLTEXT TAG INDEX nebula_index_b2 on ft_tag(prop1)
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      INSERT VERTEX ft_tag(prop1) VALUES 1:("abc");
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query:
      """
      LOOKUP ON ft_tag where prefix(ft_tag.prop1,"abc")
      YIELD id(vertex) as id, ft_tag.prop1 as prop1
      """
    Then the result should be, in any order:
      | id | prop1 |
      | 1  | "abc" |

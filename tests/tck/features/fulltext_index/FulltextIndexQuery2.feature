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
  Scenario: fulltext query2
    When executing query:
      """
      CREATE TAG tag2(prop1 string,prop2 string);
      CREATE EDGE edge2(prop1 string);
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      CREATE FULLTEXT TAG INDEX nebula_index_tag2_prop1 on tag2(prop1);
      CREATE FULLTEXT TAG INDEX nebula_index_tag2_prop2 on tag2(prop2);
      CREATE FULLTEXT EDGE INDEX nebula_index_edge2_prop1 on edge2(prop1);
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX tag2(prop1,prop2) VALUES "1":("abc","nebula graph");
      INSERT VERTEX tag2(prop1,prop2) VALUES "2":("abcde","nebula-graph");
      INSERT VERTEX tag2(prop1,prop2) VALUES "3":("bcd","nebula database");
      INSERT VERTEX tag2(prop1,prop2) VALUES "4":("zyx","Nebula");
      INSERT VERTEX tag2(prop1,prop2) VALUES "5":("cba","neBula");
      INSERT VERTEX tag2(prop1,prop2) VALUES "6":("abcxyz","nebula graph");
      INSERT VERTEX tag2(prop1,prop2) VALUES "7":("xyz","nebula graph");
      INSERT VERTEX tag2(prop1,prop2) VALUES "8":("123456","nebula graph");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT EDGE edge2(prop1) VALUES "1"->"2"@1:("一个可靠的分布式");
      INSERT EDGE edge2(prop1) VALUES "2"->"3"@3:("性能高效的图数据库");
      INSERT EDGE edge2(prop1) VALUES "3"->"4"@5:("高性能");
      INSERT EDGE edge2(prop1) VALUES "4"->"5"@7:("高吞吐");
      INSERT EDGE edge2(prop1) VALUES "5"->"6"@9:("低延时");
      INSERT EDGE edge2(prop1) VALUES "6"->"7"@11:("易扩展");
      INSERT EDGE edge2(prop1) VALUES "7"->"8"@13:("线性扩缩容");
      INSERT EDGE edge2(prop1) VALUES "8"->"1"@15:("安全稳定");
      """
    Then the execution should be successful
    And wait 10 seconds
    When executing query:
      """
      LOOKUP ON tag2 where prefix(tag2.prop1,"abc") YIELD id(vertex) as id, tag2.prop1 as prop1, tag2.prop2 as prop2
      """
    Then the result should be, in any order:
      | id  | prop1    | prop2          |
      | "1" | "abc"    | "nebula graph" |
      | "2" | "abcde"  | "nebula-graph" |
      | "6" | "abcxyz" | "nebula graph" |
    When executing query:
      """
      LOOKUP ON tag2 where prefix(tag2.prop2,"nebula") YIELD id(vertex) as id, tag2.prop1 as prop1, tag2.prop2 as prop2
      """
    Then the result should be, in any order:
      | id  | prop1    | prop2             |
      | "1" | "abc"    | "nebula graph"    |
      | "2" | "abcde"  | "nebula-graph"    |
      | "3" | "bcd"    | "nebula database" |
      | "6" | "abcxyz" | "nebula graph"    |
      | "7" | "xyz"    | "nebula graph"    |
      | "8" | "123456" | "nebula graph"    |
    When executing query:
      """
      LOOKUP ON edge2 where prefix(edge2.prop1,"高") YIELD src(edge) as src,dst(edge) as dst,rank(edge) as rank, edge2.prop1 as prop1
      """
    Then the result should be, in any order:
      | src | dst | rank | prop1    |
      | "3" | "4" | 5    | "高性能" |
      | "4" | "5" | 7    | "高吞吐" |

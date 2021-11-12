# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Fetch prop on empty tag/edge

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1 |
      | replica_factor | 1 |
    And having executed:
      """
      CREATE TAG zero_prop_tag_0();
      CREATE TAG zero_prop_tag_1();
      CREATE TAG person(money int);
      CREATE EDGE zero_prop_edge();
      """
    And having executed:
      """
      INSERT VERTEX zero_prop_tag_0() values "1":(), "2":();
      INSERT VERTEX zero_prop_tag_1() values "1":(), "2":();
      INSERT VERTEX person(money) values "1":(78), "3":(88);
      INSERT EDGE zero_prop_edge() values "1"->"2":();
      """

  Scenario: fetch prop on all tags
    When executing query:
      """
      FETCH PROP ON * '1' YIELD vertex as node
      """
    Then the result should be, in any order, with relax comparison:
      | node                                         |
      | ("1":zero_prop_tag_0:zero_prop_tag_1:person) |
    And drop the used space

  Scenario: fetch prop on a empty tag
    When executing query:
      """
      FETCH PROP ON zero_prop_tag_0 '1' YIELD vertex as node
      """
    Then the result should be, in any order, with relax comparison:
      | node                  |
      | ("1":zero_prop_tag_0) |
    When executing query:
      """
      GO FROM "1" OVER zero_prop_edge YIELD zero_prop_edge._dst as id |
      FETCH PROP ON zero_prop_tag_0 $-.id YIELD vertex as node
      """
    Then the result should be, in any order, with relax comparison:
      | node                  |
      | ("2":zero_prop_tag_0) |
    And drop the used space

  Scenario: fetch prop on empty edge
    When executing query:
      """
      FETCH PROP ON zero_prop_edge "1"->"2" YIELD edge as e
      """
    Then the result should be, in any order:
      | e                               |
      | [:zero_prop_edge "1"->"2" @0{}] |
    When executing query:
      """
      FETCH PROP ON zero_prop_edge "1"->"3" YIELD edge as e
      """
    Then the result should be, in any order:
      | e |
    When executing query:
      """
      FETCH PROP ON zero_prop_edge "101"->"102" YIELD edge as e
      """
    Then the result should be, in any order:
      | e |
    When executing query:
      """
      GO FROM "1" OVER zero_prop_edge YIELD zero_prop_edge._src as src, zero_prop_edge._dst as dst |
      FETCH PROP ON zero_prop_edge $-.src->$-.dst YIELD edge as e
      """
    Then the result should be, in any order:
      | e                               |
      | [:zero_prop_edge "1"->"2" @0{}] |
    And drop the used space

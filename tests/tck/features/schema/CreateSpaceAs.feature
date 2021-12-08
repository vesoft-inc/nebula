# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Create space as another space

  Scenario: clone space
    # Space
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    # Schema
    When executing query:
      """
      create tag t1 (col1 int);
      """
    Then the execution should be successful
    When executing query:
      """
      create tag index i1 on t1(col1);
      """
    Then the execution should be successful
    When executing query:
      """
      create edge e1 (col1 int);
      """
    Then the execution should be successful
    When executing query:
      """
      create edge index i2 on e1(col1);
      """
    Then the execution should be successful
    And wait 3 seconds
    # insert
    When executing query:
      """
      insert vertex t1(col1) values "1": (1)
      """
    Then the execution should be successful
    When executing query:
      """
      insert edge e1(col1) VALUES "1" -> "2":(1);
      """
    Then the execution should be successful
    # query
    When executing query:
      """
      fetch prop on t1 "1" YIELD vertex as node;
      """
    Then the result should be, in any order:
      | node               |
      | ("1" :t1{col1: 1}) |
    When executing query:
      """
      lookup on t1 where t1.col1 == 1 YIELD id(vertex) as id;
      """
    Then the result should be, in any order:
      | id  |
      | "1" |
    When executing query:
      """
      fetch prop on e1 "1" -> "2" YIELD edge as e;
      """
    Then the result should be, in any order:
      | e                           |
      | [:e1 "1"->"2" @0 {col1: 1}] |
    When executing query:
      """
      lookup on e1 where e1.col1 == 1 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank;
      """
    Then the result should be, in any order:
      | src | dst | rank |
      | "1" | "2" | 0    |
    # clone space
    When clone a new space according to current space
    And wait 3 seconds
    Then the execution should be successful
    # check schema is really cloned
    When executing query:
      """
      show tags;
      """
    Then the result should be, in any order:
      | Name |
      | "t1" |
    When executing query:
      """
      show edges;
      """
    Then the result should be, in any order:
      | Name |
      | "e1" |
    When executing query:
      """
      show create edge e1;
      """
    Then the result should be, in any order:
      | Edge | Create Edge                                                                |
      | "e1" | 'CREATE EDGE `e1` (\n `col1` int64 NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      show tag indexes;
      """
    Then the result should be, in any order:
      | Index Name | By Tag | Columns  |
      | "i1"       | "t1"   | ["col1"] |
    When executing query:
      """
      show edge indexes;
      """
    Then the result should be, in any order:
      | Index Name | By Edge | Columns  |
      | "i2"       | "e1"    | ["col1"] |
    # check no data in new space
    When executing query:
      """
      fetch prop on t1 "1" YIELD vertex as node;
      """
    Then the result should be, in any order:
      | node |
    When executing query:
      """
      fetch prop on e1 "1" -> "2" YIELD edge as e;
      """
    Then the result should be, in any order:
      | e |
    # write new data into cloned space
    When executing query:
      """
      insert vertex t1(col1) values "1": (2)
      """
    Then the execution should be successful
    When executing query:
      """
      insert edge e1(col1) VALUES "1" -> "2":(2);
      """
    # query
    When executing query:
      """
      fetch prop on t1 "1" YIELD vertex as node;
      """
    Then the result should be, in any order:
      | node               |
      | ("1" :t1{col1: 2}) |
    When executing query:
      """
      lookup on t1 where t1.col1 == 2 YIELD id(vertex) as id;
      """
    Then the result should be, in any order:
      | id  |
      | "1" |
    When executing query:
      """
      fetch prop on e1 "1" -> "2" YIELD edge as e;
      """
    Then the result should be, in any order:
      | e                           |
      | [:e1 "1"->"2" @0 {col1: 2}] |
    When executing query:
      """
      lookup on e1 where e1.col1 == 2 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank;
      """
    Then the result should be, in any order:
      | src | dst | rank |
      | "1" | "2" | 0    |
    Then drop the used space

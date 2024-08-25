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

  Scenario: Tag Fixed String Property
    When executing query:
      """
      CREATE TAG tag_with_fixed_string(col1 fixed_string(5));
      """
    And wait 5 seconds
    When executing query:
      """
      INSERT VERTEX tag_with_fixed_string(col1)
      VALUES
        "1": ("😀😀"),
        "2": ("😂😂"),
        "3": ("羊羊羊"),
        "4": ("🐏🐏🐏");
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP on tag_with_fixed_string "1" yield tag_with_fixed_string.col1 as col1
      """
    Then the result should be, in any order:
      | col1 |
      | "😀" |
    When executing query:
      """
      FETCH PROP on tag_with_fixed_string "2" yield tag_with_fixed_string.col1 as col1
      """
    Then the result should be, in any order:
      | col1 |
      | "😂" |
    When executing query:
      """
      FETCH PROP on tag_with_fixed_string "3" yield tag_with_fixed_string.col1 as col1
      """
    Then the result should be, in any order:
      | col1 |
      | "羊" |
    When executing query:
      """
      FETCH PROP on tag_with_fixed_string "4" yield tag_with_fixed_string.col1 as col1
      """
    Then the result should be, in any order:
      | col1 |
      | "🐏" |
    And drop the used space

  Scenario: Edge Fixed String Property
    When executing query:
      """
      CREATE EDGE edge_with_fixed_string(col1 fixed_string(5));
      """
    And wait 5 seconds
    When executing query:
      """
      INSERT EDGE edge_with_fixed_string(col1)
      VALUES
        "1" -> "1": ("😀😀"),
        "2" -> "2": ("😂😂"),
        "3" -> "3": ("羊羊羊"),
        "4" -> "4": ("🐏🐏🐏");
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP on edge_with_fixed_string "1" -> "1" yield edge_with_fixed_string.col1 as col1
      """
    Then the result should be, in any order:
      | col1 |
      | "😀" |
    When executing query:
      """
      FETCH PROP on edge_with_fixed_string "2" -> "2" yield edge_with_fixed_string.col1 as col1
      """
    Then the result should be, in any order:
      | col1 |
      | "😂" |
    When executing query:
      """
      FETCH PROP on edge_with_fixed_string "3" -> "3" yield edge_with_fixed_string.col1 as col1
      """
    Then the result should be, in any order:
      | col1 |
      | "羊" |
    When executing query:
      """
      FETCH PROP on edge_with_fixed_string "4" -> "4" yield edge_with_fixed_string.col1 as col1
      """
    Then the result should be, in any order:
      | col1 |
      | "🐏" |
    And drop the used space

  Scenario: Fetch hobby property from player tag
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE TAG player(name string, age int, hobby List< string >, ids List< int >, score List< float >);
      """
    Then the execution should be successful
    
    # Ensure the tag is successfully created
    And wait 3 seconds
    
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, ["Basketball", "Swimming"], [1, 2], [9.0, 8.0]);
      """
    Then the execution should be successful
    
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD properties(vertex).hobby AS hobby;
      """
    Then the result should be, in any order, with relax comparison:
      | hobby                      |
      | ["Basketball", "Swimming"] |

  Scenario: Fetch hobby property from player tag
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE TAG player(name string, age int, hobby Set< string >, ids Set< int >, score Set< float >);
      """
    Then the execution should be successful
    
    # Ensure the tag is successfully created
    And wait 3 seconds
    
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, {"Basketball", "Swimming", "Basketball"}, {1, 2, 1}, {9.0, 8.0, 9.0});
      """
    Then the execution should be successful
    
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD properties(vertex).hobby AS hobby, properties(vertex).ids AS ids, properties(vertex).score AS score;
      """
    Then the result should be, in any order, with relax comparison:
      | hobby                      | ids      | score         |
      | {"Basketball", "Swimming"} | {1, 2}   | {8.0, 9.0}    |

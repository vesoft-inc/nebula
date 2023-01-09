# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Index selecting for match statement

  Background: Prepare a new space
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE tag player(name string, age int, score int, gender bool);
      """
    And having executed:
      """
      INSERT VERTEX player(name, age, score, gender) VALUES "Tim Duncan":("Tim Duncan", 42, 28, true),"Yao Ming":("Yao Ming", 38, 23, true),"Nneka Ogwumike":("Nneka Ogwumike", 35, 13, false);
      """
    And having executed:
      """
      create tag index player_index on player();
      create tag index player_name_index on player(name(8));
      create tag index player_age_name_index on player(age,name(8));
      """
    And wait 3 seconds

  Scenario: Test Index selecting
    When submit a job:
      """
      rebuild tag index player_index;
      """
    Then wait the job to finish
    When submit a job:
      """
      rebuild tag index player_name_index;
      """
    Then wait the job to finish
    When submit a job:
      """
      rebuild tag index player_age_name_index;
      """
    Then wait the job to finish
    # Prefix Index
    When profiling query:
      """
      MATCH (v:player {name: "Yao Ming"}) RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name       |
      | "Yao Ming" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                       |
      | 6  | Project        | 2            |                                                     |
      | 2  | AppendVertices | 5            |                                                     |
      | 5  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 0  | Start          |              |                                                     |
    When profiling query:
      """
      MATCH (v:player) WHERE v.player.name in ["Yao Ming"] RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name       |
      | "Yao Ming" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                       |
      | 6  | Project        | 2            |                                                     |
      | 2  | Filter         | 5            |                                                     |
      | 2  | AppendVertices | 5            |                                                     |
      | 5  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 0  | Start          |              |                                                     |
    When profiling query:
      """
      MATCH (v:player) WHERE v.player.name in ["Yao Ming", "Tim Duncan"] RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name         |
      | "Tim Duncan" |
      | "Yao Ming"   |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                       |
      | 6  | Project        | 2            |                                                     |
      | 2  | Filter         | 5            |                                                     |
      | 2  | AppendVertices | 5            |                                                     |
      | 5  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 0  | Start          |              |                                                     |
    When profiling query:
      """
      MATCH (v:player) WHERE v.player.name == "Tim Duncan" and v.player.name < "Zom" RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name         |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                       |
      | 9  | Project        | 7            |                                                     |
      | 7  | Filter         | 2            |                                                     |
      | 2  | AppendVertices | 6            |                                                     |
      | 6  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 0  | Start          |              |                                                     |
    When profiling query:
      """
      MATCH (v:player) WHERE v.player.name=="Tim Duncan" and v.player.age>4 and v.player.name>"A" RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name         |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                       |
      | 9  | Project        | 7            |                                                     |
      | 7  | Filter         | 2            |                                                     |
      | 2  | AppendVertices | 6            |                                                     |
      | 6  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 0  | Start          |              |                                                     |
    When profiling query:
      """
      MATCH (v:player{name:"Tim Duncan"}) WHERE v.player.name < "Zom" RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name         |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                       |
      | 9  | Project        | 7            |                                                     |
      | 7  | Filter         | 2            |                                                     |
      | 2  | AppendVertices | 6            |                                                     |
      | 6  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 0  | Start          |              |                                                     |
    # Range Index
    When profiling query:
      """
      MATCH (v:player) WHERE v.player.name > "Tim" and v.player.name < "Zom" RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name         |
      | "Yao Ming"   |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                      |
      | 9  | Project        | 7            |                                                    |
      | 7  | Filter         | 2            |                                                    |
      | 2  | AppendVertices | 6            |                                                    |
      | 6  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"RANGE"}}} |
      | 0  | Start          |              |                                                    |
    # Degeneration to FullScan Index
    When executing query:
      """
      MATCH (v:player) WHERE v.player.score < 20 RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name             |
      | "Nneka Ogwumike" |
    # Degeneration to Prefix Index
    When profiling query:
      """
      MATCH (v:player) WHERE v.player.name == "Tim Duncan" and v.player.score == 28 RETURN v.player.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name         |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                       |
      | 9  | Project        | 7            |                                                     |
      | 7  | Filter         | 2            |                                                     |
      | 2  | AppendVertices | 6            |                                                     |
      | 6  | IndexScan      | 0            | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 0  | Start          |              |                                                     |
    Then drop the used space

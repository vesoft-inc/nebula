# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Clear space test
  This file is mainly used to test the function of clear space.

  Scenario: Clear space syntax test
    Given an empty graph
    And create a space with following options:
      | name           | clear_space      |
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And wait 2 seconds
    When executing query:
      """
      CLEAR SPACE IF EXISTS clear_space;
      """
    Then the execution should be successful
    When executing query:
      """
      CLEAR SPACE clear_space;
      """
    Then the execution should be successful
    When executing query:
      """
      CLEAR SPACE IF EXISTS clear_space_0;
      """
    Then the execution should be successful
    When executing query:
      """
      CLEAR SPACE clear_space_0;
      """
    Then a ExecutionError should be raised at runtime: Space not existed!

  Scenario: Clear space function test
    Given an empty graph
    And create a space with following options:
      | name           | clear_space      |
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And wait 2 seconds
    When executing query:
      """
      CREATE TAG IF NOT EXISTS player(name string, age int);
      CREATE TAG INDEX IF NOT EXISTS name ON player(name(20));
      """
    And wait 2 seconds
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX player(name, age) VALUES
            "Russell Westbrook": ("Russell Westbrook", 30),
            "Chris Paul": ("Chris Paul", 33),
            "Boris Diaw": ("Boris Diaw", 36),
            "David West": ("David West", 38),
            "Danny Green": ("Danny Green", 31),
            "Tim Duncan": ("Tim Duncan", 42),
            "James Harden": ("James Harden", 29),
            "Tony Parker": ("Tony Parker", 36),
            "Aron Baynes": ("Aron Baynes", 32),
            "Ben Simmons": ("Ben Simmons", 22),
            "Blake Griffin": ("Blake Griffin", 30);
      """
    Then the execution should be successful
    When executing query:
      """
      submit job stats;
      """
    And wait 2 seconds
    Then the execution should be successful
    When executing query:
      """
      show stats;
      """
    Then the execution should be successful
    And the result should be, in any order, with relax comparison:
      | Type    | Name       | Count |
      | "Tag"   | "player"   | 11    |
      | "Space" | "vertices" | 11    |
      | "Space" | "edges"    | 0     |
    When executing query:
      """
      CLEAR SPACE IF EXISTS clear_space;
      """
    Then the execution should be successful
    When executing query:
      """
      submit job stats;
      """
    And wait 2 seconds
    Then the execution should be successful
    When executing query:
      """
      show stats;
      """
    Then the execution should be successful
    And the result should be, in any order, with relax comparison:
      | Type    | Name       | Count |
      | "Tag"   | "player"   | 0     |
      | "Space" | "vertices" | 0     |
      | "Space" | "edges"    | 0     |
    # permission test
    When executing query:
      """
      CREATE USER IF NOT EXISTS clear_space_user WITH PASSWORD 'nebula';
      GRANT ROLE ADMIN ON clear_space TO clear_space_user;
      """
    And wait 2 seconds
    Then the execution should be successful
    When connect to nebula service with user[u:clear_space_user, p:nebula]
    And executing clear space
    Then the result should be failed

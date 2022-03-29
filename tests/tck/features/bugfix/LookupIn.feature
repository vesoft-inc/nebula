# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Lookup_In

  # Lookup ... WHERE xxx IN xxx causes service crash
  # Fix https://github.com/vesoft-inc/nebula/issues/3983
  Scenario: lookup in where prop has no index
    Given an empty graph
    And load "nba" csv data to a new space
    When executing query:
      """
      DROP TAG INDEX IF EXISTS player_age_index
      """
    Then the execution should be successful
    And wait 6 seconds
    When submit a job:
      """
      REBUILD TAG INDEX
      """
    Then wait the job to finish
    When executing query:
      """
      Show TAG INDEXES;
      """
    Then the result should be, in any order:
      | Index Name          | By Tag     | Columns  |
      | "bachelor_index"    | "bachelor" | []       |
      | "player_name_index" | "player"   | ["name"] |
      | "team_name_index"   | "team"     | ["name"] |
    # player.age has no index
    When executing query:
      """
      LOOKUP ON player WHERE player.age IN [40, 20] AND player.name > "" YIELD id(vertex) as id, player.age
      """
    Then the result should be, in any order:
      | id              | player.age |
      | "Dirk Nowitzki" | 40         |
      | "Luka Doncic"   | 20         |
      | "Kobe Bryant"   | 40         |
    Then drop the used space

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: FixIterCrash

  Scenario: Fix GetNeighborsIter Crash
    Given an empty graph
    And having executed:
      """
      CREATE SPACE nba_FixIterCrash as nba
      """
    And wait 6 seconds
    And having executed:
      """
      USE nba_FixIterCrash
      """
    When executing query:
      """
      GO from 'Tim Duncan' OVER serve YIELD serve._src AS id |
      GET SUBGRAPH WITH PROP FROM $-.id
      YIELD vertices as nodes, edges as relationships
      """
    Then the execution should be successful

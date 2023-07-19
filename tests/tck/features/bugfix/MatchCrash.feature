# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: match bug fix

  Background:
    Given a graph with space named "nba"

  Scenario: match crash due to where in with
    When try to execute query:
      """
      MATCH (n0)-[e0]->(n1:player{age: 102, in_service: false})
      WHERE (id(n0) IN ["Tim Duncan"])
      WITH MIN(87) AS a0, n1.player.served_years AS a1
      WHERE a1 == 100
      RETURN *
      """
    Then the execution should be successful

# Copyright (c) 2024 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: label index bug fix

  Background:
    Given a graph with space named "nba"

  @testmark
  Scenario: attribute expression encode crash
    When try to execute query:
      """
      MATCH (x:bachelor)
      WHERE x.bachelor.name == "Tim Duncan"
      or x.name == "Tim Duncan"
      RETURN x.bachelor.name;
      """
    Then the result should be, in any order:
      | x.bachelor.name |
      | "Tim Duncan"    |
    When try to execute query:
      """
      MATCH (v:bachelor)-[e:serve]-(v2)
      WHERE v.bachelor.name == "Tim Duncan" or e.start_year > 2000
      RETURN v.bachelor.name,e.start_year
      """
    Then the result should be, in any order:
      | v.bachelor.name | e.start_year |
      | "Tim Duncan"    | 1997         |

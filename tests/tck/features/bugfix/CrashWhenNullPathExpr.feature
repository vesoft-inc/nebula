# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test crash when null path expression

  Background:
    Given a graph with space named "nba"

  Scenario: Null path expression in multiple patterns
    When executing query:
      """
      MATCH (p:player {name: 'Yao Ming'} ), (t:team {name: 'Rockets'}), pth = (p)-[:serve*1..4]-(t)
      RETURN pth
      """
    Then the result should be, in any order, with relax comparison:
      | pth                                                                                                                       |
      | <("Yao Ming":player{age:38,name:"Yao Ming"})-[:serve@0{end_year:2011,start_year:2002}]->("Rockets":team{name:"Rockets"})> |

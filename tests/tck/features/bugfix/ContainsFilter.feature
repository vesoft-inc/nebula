# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: contains filter

  Background:
    Given a graph with space named "nba"

  Scenario: contains filter
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[e]-(m) where m.player.name contains "Tony Parker" RETURN n,e,m  ORDER BY m;
      """
    Then the result should be, in any order, with relax comparison:
      | n                                                  | e                                                  | m                                                   |
      | ("player100" :player{age: 42, name: "Tim Duncan"}) | [:follow "player101"->"player100" @0 {degree: 95}] | ("player101" :player{age: 36, name: "Tony Parker"}) |
      | ("player100" :player{age: 42, name: "Tim Duncan"}) | [:follow "player100"->"player101" @0 {degree: 95}] | ("player101" :player{age: 36, name: "Tony Parker"}) |
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[e]-(m) where m.player.name starts with "Manu" RETURN n,e,m  ORDER BY m;
      """
    Then the result should be, in any order, with relax comparison:
      | n                                                  | e                                                  | m                                                     |
      | ("player100" :player{age: 42, name: "Tim Duncan"}) | [:follow "player125"->"player100" @0 {degree: 90}] | ("player125" :player{age: 41, name: "Manu Ginobili"}) |
      | ("player100" :player{age: 42, name: "Tim Duncan"}) | [:follow "player100"->"player125" @0 {degree: 95}] | ("player125" :player{age: 41, name: "Manu Ginobili"}) |
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[e]-(m) where m.team.name ends with "urs" RETURN n,e,m  ORDER BY m;
      """
    Then the result should be, in any order, with relax comparison:
      | n                                                  | e                                                                     | m                                |
      | ("player100" :player{age: 42, name: "Tim Duncan"}) | [:serve "player100"->"team204" @0 {end_year: 2016, start_year: 1997}] | ("team204" :team{name: "Spurs"}) |
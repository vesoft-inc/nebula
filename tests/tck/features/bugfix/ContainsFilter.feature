# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
@tag1
Feature: contains filter

  Background:
    Given a graph with space named "nba"

  Scenario: contains filter
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[e]->(m) where m.player.name contains "Tony Parker" RETURN n,e,m  ORDER BY m;
      """
    Then the result should be, in order:
      | n                                                                                                           | e                                                                             | m                                                     |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]                         | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}] | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[e]->(m) where m.player.name starts with "Manu" RETURN n,e,m  ORDER BY m;
      """
    Then the result should be, in order:
      | n                                                                                                           | e                                                                               | m                                                         |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]                         | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}] | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[e]->(m) where m.team.name ends with "urs" RETURN n,e,m  ORDER BY m;
      """
    Then the result should be, in order:
      | n                                                                                                           | e                                                                    | m                              |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}] | ("Spurs" :team{name: "Spurs"}) |

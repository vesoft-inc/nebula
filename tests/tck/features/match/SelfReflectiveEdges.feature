# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Matches on self-reflective edges

  Scenario: no duplicated self reflective edges
    Given a graph with space named "nba"
    And having executed:
      """
      insert vertex player (name, age) values "Hades":("Hades", 99999);
      insert vertex team (name) values "Underworld":("Underworld");
      insert edge like (likeness) values "Hades"->"Hades":(3000);
      insert edge teammate (start_year, end_year) values "Hades"->"Hades":(3000, 3000);
      insert edge serve (start_year, end_year) values "Hades"->"Underworld":(0, 99999);
      """
    When executing query:
      """
      MATCH x = (n0)-[e1]->(n1)-[e2]-(n0) WHERE id(n0) == "Hades" and id(n1) == "Hades" return e1, e2
      """
    Then the result should be, in any order:
      | e1                                                                 | e2                                                                 |
      | [:teammate "Hades"->"Hades" @0 {end_year: 3000, start_year: 3000}] | [:like "Hades"->"Hades" @0 {likeness: 3000}]                       |
      | [:like "Hades"->"Hades" @0 {likeness: 3000}]                       | [:teammate "Hades"->"Hades" @0 {end_year: 3000, start_year: 3000}] |
    When executing query:
      """
      MATCH x = (n0)-[e1]->(n1)<-[e2]-(n0) WHERE id(n0) == "Hades" and id(n1) == "Hades" return e1, e2
      """
    Then the result should be, in any order:
      | e1                                                                 | e2                                                                 |
      | [:teammate "Hades"->"Hades" @0 {end_year: 3000, start_year: 3000}] | [:like "Hades"->"Hades" @0 {likeness: 3000}]                       |
      | [:like "Hades"->"Hades" @0 {likeness: 3000}]                       | [:teammate "Hades"->"Hades" @0 {end_year: 3000, start_year: 3000}] |
    When executing query:
      """
      DELETE EDGE serve "Hades"->"Underworld";
      DELETE EDGE teammate "Hades"->"Hades";
      DELETE EDGE like "Hades"->"Hades";
      DELETE VERTEX "Underworld";
      DELETE VERTEX "Hades";
      """
    Then the execution should be successful

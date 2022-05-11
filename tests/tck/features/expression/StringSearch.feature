# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Starts With Expression

  Background:
    Given a graph with space named "nba"

  Scenario: yield starts with
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "app") |
      | true                        |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "a") |
      | true                      |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'A'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "A") |
      | false                     |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "b") |
      | false                     |
    When executing query:
      """
      YIELD '123' STARTS WITH '1'
      """
    Then the result should be, in any order:
      | ("123" STARTS WITH "1") |
      | true                    |
    When executing query:
      """
      YIELD 123 STARTS WITH 1
      """
    Then a SemanticError should be raised at runtime: Type error `(123 STARTS WITH 1)'

  Scenario: yield not starts with
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "app") |
      | false                           |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "a") |
      | false                         |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'A'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "A") |
      | true                          |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "b") |
      | true                          |
    When executing query:
      """
      YIELD '123' NOT STARTS WITH '1'
      """
    Then the result should be, in any order:
      | ("123" NOT STARTS WITH "1") |
      | false                       |
    When executing query:
      """
      YIELD 123 NOT STARTS WITH 1
      """
    Then a SemanticError should be raised at runtime: Type error `(123 NOT STARTS WITH 1)'

  Scenario: starts with
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst STARTS WITH 'LaMarcus' YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst STARTS WITH 'Obama' YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER * WHERE $$.player.name STARTS WITH 'T' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                             |
      | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}] |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                         |
    When executing query:
      """
      GO 2 STEPS FROM 'Tim Duncan' OVER * WHERE $$.player.name STARTS WITH 'M' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                                |
      | [:like "Danny Green"->"Marco Belinelli" @0 {likeness: 83}]                       |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                         |
      | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}] |
    When executing query:
      """
      MATCH p = (a:player)-[*..2]-(b) WHERE a.player.age > 40 AND b.player.name STARTS WITH "Y" RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                                                                                                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                               |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})<-[:like@0 {likeness: 90}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                               |
      | <("Vince Carter" :player{age: 42, name: "Vince Carter"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                           |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                                                                                                       |
    When executing query:
      """
      MATCH p = (a:player)-[]->(b) WHERE a.player.name STARTS WITH "Y" RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})>       |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})> |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})>         |

  Scenario: not starts
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT STARTS WITH 'T'
      YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
      | 'Tony Parker'  |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like
      WHERE like.likeness NOT IN [95,56,21] AND $$.player.name NOT STARTS WITH 'Tony'
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name      | $$.player.name | like.likeness |
      | 'Manu Ginobili'     | 'Tim Duncan'   | 90            |
      | 'LaMarcus Aldridge' | 'Tim Duncan'   | 75            |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like
      WHERE like.likeness NOT IN [95,56,21] AND $^.player.name NOT STARTS WITH 'LaMarcus'
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name  | $$.player.name | like.likeness |
      | 'Manu Ginobili' | 'Tim Duncan'   | 90            |

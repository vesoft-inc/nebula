# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down Project rule

  Background:
    Given a graph with space named "nba"

  # TODO(yee):
  Scenario: push filter down Project
    When profiling query:
      """
      MATCH (a:player)--(b)--(c)
      WITH a AS a, b AS b, c AS c
      WHERE a.player.age < 25 AND b.player.age < 25 AND c.player.age < 25
      RETURN a
      """
    Then the result should be, in any order:
      | a                                                                   |
      | ("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"}) |
      | ("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"}) |
      | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"})               |
      | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"})               |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 14 | Project        | 11           |               |
      | 11 | Filter         | 5            |               |
      | 5  | AppendVertices | 4            |               |
      | 4  | Traverse       | 13           |               |
      | 13 | Traverse       | 1            |               |
      | 1  | IndexScan      | 2            |               |
      | 2  | Start          |              |               |
    When profiling query:
      """
      MATCH (a:player)--(b)--(c)
      WITH a, b, c.player.age+1 AS cage
      WHERE a.player.name == 'Tim Duncan' AND b.player.age > 40
      RETURN DISTINCT a, b, cage
      """
    Then the result should be, in any order:
      | a                                                                                                           | b                                                               | cage |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"}) | 39   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"}) | 32   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"}) | NULL |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})       | NULL |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})       | 43   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})       | 37   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})       | 35   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})       | 30   |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 10 | Dedup          | 15           |               |
      | 15 | Project        | 12           |               |
      | 12 | Filter         | 5            |               |
      | 5  | AppendVertices | 4            |               |
      | 4  | Traverse       | 14           |               |
      | 14 | Traverse       | 1            |               |
      | 1  | IndexScan      | 2            |               |
      | 2  | Start          |              |               |

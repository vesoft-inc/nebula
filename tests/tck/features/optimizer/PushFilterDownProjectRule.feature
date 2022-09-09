# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down Project rule

  Background:
    Given a graph with space named "nba"

  # TODO(yee):
  @skip
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
      | id | name         | dependencies | operator info |
      | 23 | Project      | 40           |               |
      | 40 | Project      | 39           |               |
      | 39 | Filter       | 20           |               |
      | 20 | Filter       | 19           |               |
      | 19 | Project      | 18           |               |
      | 18 | InnerJoin    | 17           |               |
      | 17 | Project      | 28           |               |
      | 28 | GetVertices  | 13           |               |
      | 13 | InnerJoin    | 12           |               |
      | 12 | Filter       | 11           |               |
      | 11 | Project      | 32           |               |
      | 32 | GetNeighbors | 7            |               |
      | 7  | Filter       | 6            |               |
      | 6  | Project      | 5            |               |
      | 5  | Filter       | 31           |               |
      | 31 | GetNeighbors | 24           |               |
      | 24 | IndexScan    | 0            |               |
      | 0  | Start        |              |               |
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
      | id | name         | dependencies | operator info |
      | 24 | Dedup        | 41           |               |
      | 41 | Project      | 40           |               |
      | 40 | Filter       | 20           |               |
      | 20 | Filter       | 19           |               |
      | 19 | Project      | 18           |               |
      | 18 | InnerJoin    | 17           |               |
      | 17 | Project      | 29           |               |
      | 29 | GetVertices  | 13           |               |
      | 13 | InnerJoin    | 12           |               |
      | 12 | Filter       | 11           |               |
      | 11 | Project      | 33           |               |
      | 33 | GetNeighbors | 7            |               |
      | 7  | Filter       | 6            |               |
      | 6  | Project      | 5            |               |
      | 5  | Filter       | 32           |               |
      | 32 | GetNeighbors | 26           |               |
      | 26 | IndexScan    | 0            |               |
      | 0  | Start        |              |               |

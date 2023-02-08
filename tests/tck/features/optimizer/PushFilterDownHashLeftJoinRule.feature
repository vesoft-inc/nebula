# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down HashLeftJoin rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down HashLeftJoin
    When profiling query:
      """
      GO FROM 'Tony Parker' OVER like
        WHERE like.likeness > 90
        YIELD $$ as dst, edge as e
      """
    Then the result should be, in any order:
      | dst                                                                                                         | e                                                        |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}] |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]    |
    And the execution plan should be:
      | id | name         | dependencies | operator info                    |
      | 8  | Project      | 11           |                                  |
      | 11 | HashLeftJoin | 13,5         |                                  |
      | 13 | Project      | 14           |                                  |
      | 14 | GetNeighbors | 0            | {"filter": "(like.likeness>90)"} |
      | 0  | Start        |              |                                  |
      | 5  | Project      | 4            |                                  |
      | 4  | GetVertices  | 3            |                                  |
      | 3  | Argument     |              |                                  |
    When profiling query:
      """
      GO 2 STEPS FROM 'Tony Parker' OVER like
        WHERE like.likeness > 90
        YIELD $$ as dst, edge as e
      """
    Then the result should be, in any order:
      | dst                                                       | e                                                       |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    And the execution plan should be:
      | id | name         | dependencies | operator info                    |
      | 12 | Project      | 15           |                                  |
      | 15 | HashLeftJoin | 17,9         |                                  |
      | 17 | Project      | 18           |                                  |
      | 18 | GetNeighbors | 4            | {"filter": "(like.likeness>90)"} |
      | 4  | Loop         | 0            |                                  |
      | 3  | Dedup        | 2            |                                  |
      | 2  | GetDstBySrc  | 1            |                                  |
      | 1  | Start        |              |                                  |
      | 0  | Start        |              |                                  |
      | 9  | Project      | 8            |                                  |
      | 8  | GetVertices  | 7            |                                  |
      | 7  | Argument     |              |                                  |
    When profiling query:
      """
      $hop1 = GO FROM 'Tony Parker' OVER * BIDIRECT YIELD DISTINCT id($$) as dst;
      $hop2 = GO FROM $hop1.dst OVER * BIDIRECT WHERE like.likeness > 80 OR like.likeness IS EMPTY
        YIELD DISTINCT id($^) as src, id($$) as dst, tags($$) as tag_list
        | ORDER BY $-.src, $-.dst | LIMIT 5
      """
    Then the result should be, in any order:
      | src          | dst       | tag_list |
      | "Boris Diaw" | "Hawks"   | ["team"] |
      | "Boris Diaw" | "Hornets" | ["team"] |
      | "Boris Diaw" | "Jazz"    | ["team"] |
      | "Boris Diaw" | "Spurs"   | ["team"] |
      | "Boris Diaw" | "Suns"    | ["team"] |
    And the execution plan should be:
      | id | name         | dependencies | operator info                                                |
      | 19 | TopN         | 14           |                                                              |
      | 14 | Dedup        | 13           |                                                              |
      | 13 | Project      | 21           |                                                              |
      | 21 | InnerJoin    | 24           |                                                              |
      | 24 | HashLeftJoin | 26,9         |                                                              |
      | 26 | Project      | 27           |                                                              |
      | 27 | GetNeighbors | 2            | {"filter": "((like.likeness>80) OR like.likeness IS EMPTY)"} |
      | 2  | Dedup        | 1            |                                                              |
      | 1  | GetDstBySrc  | 0            |                                                              |
      | 0  | Start        |              |                                                              |
      | 9  | Project      | 8            |                                                              |
      | 8  | GetVertices  | 7            |                                                              |
      | 7  | Argument     |              |                                                              |

  Scenario: NOT push filter down HashLeftJoin
    When profiling query:
      """
      GO FROM 'Tony Parker' OVER like
        WHERE like.likeness > 90 OR $$.player.age > 30
        YIELD $$ as dst, edge as e
      """
    Then the result should be, in any order:
      | dst                                                                                                         | e                                                            |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    And the execution plan should be:
      | id | name         | dependencies | operator info                    |
      | 8  | Project      | 7            |                                  |
      | 7  | Filter       | 6            |                                  |
      | 6  | HashLeftJoin | 2,5          |                                  |
      | 2  | Project      | 1            |                                  |
      | 1  | GetNeighbors | 0            | {"filter": "(like.likeness>90)"} |
      | 0  | Start        |              |                                  |
      | 5  | Project      | 4            |                                  |
      | 4  | GetVertices  | 3            |                                  |
      | 3  | Argument     |              |                                  |

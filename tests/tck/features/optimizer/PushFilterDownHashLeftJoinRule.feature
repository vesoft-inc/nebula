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
      | 9  | Project      | 12           |                                  |
      | 12 | HashLeftJoin | 13,6         |                                  |
      | 13 | ExpandAll    | 2            | {"filter": "(like.likeness>90)"} |
      | 2  | Expand       | 1            |                                  |
      | 1  | Start        |              |                                  |
      | 6  | Project      | 5            |                                  |
      | 5  | GetVertices  | 4            |                                  |
      | 4  | Argument     |              |                                  |
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
      | 9  | Project      | 12           |                                  |
      | 12 | HashLeftJoin | 13,6         |                                  |
      | 13 | ExpandAll    | 2            | {"filter": "(like.likeness>90)"} |
      | 2  | Expand       | 1            |                                  |
      | 1  | Start        |              |                                  |
      | 6  | Project      | 5            |                                  |
      | 5  | GetVertices  | 4            |                                  |
      | 4  | Argument     |              |                                  |
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
      | id | name          | dependencies | operator info                                                |
      | 20 | TopN          | 17           |                                                              |
      | 17 | Dedup         | 16           |                                                              |
      | 16 | Project       | 23           |                                                              |
      | 23 | HashInnerJoin | 5,26         |                                                              |
      | 5  | Dedup         | 4            |                                                              |
      | 4  | Project       | 3            |                                                              |
      | 3  | ExpandAll     | 2            |                                                              |
      | 2  | Expand        | 1            |                                                              |
      | 1  | Start         |              |                                                              |
      | 26 | HashLeftJoin  | 27,12        |                                                              |
      | 27 | ExpandAll     | 8            | {"filter": "((like.likeness>80) OR like.likeness IS EMPTY)"} |
      | 8  | Expand        | 7            |                                                              |
      | 7  | Argument      |              |                                                              |
      | 12 | Project       | 11           |                                                              |
      | 11 | GetVertices   | 10           |                                                              |
      | 10 | Argument      |              |                                                              |

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
      | id | name         | dependencies | operator info                                     |
      | 9  | Project      | 8            |                                                   |
      | 8  | Filter       | 7            | {"condition": "(($__COL_0>90) OR ($__COL_1>30))"} |
      | 7  | HashLeftJoin | 3,6          |                                                   |
      | 3  | ExpandAll    | 2            |                                                   |
      | 2  | Expand       | 1            |                                                   |
      | 1  | Start        |              |                                                   |
      | 6  | Project      | 5            |                                                   |
      | 5  | GetVertices  | 4            |                                                   |
      | 4  | Argument     |              |                                                   |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE $$.player.age >= 32 AND like.likeness > 85
      YIELD like._dst AS id, like.likeness AS likeness, $$.player.age AS age
      """
    Then the result should be, in any order:
      | id                  | likeness | age |
      | "LaMarcus Aldridge" | 90       | 33  |
      | "Manu Ginobili"     | 95       | 41  |
      | "Tim Duncan"        | 95       | 42  |
    And the execution plan should be:
      | id | name         | dependencies | operator info                    |
      | 9  | Project      | 8            |                                  |
      | 8  | Filter       | 7            | {"condition": "($__COL_0>=32)"}  |
      | 7  | HashLeftJoin | 3,6          |                                  |
      | 3  | ExpandAll    | 2            | {"filter": "(like.likeness>85)"} |
      | 2  | Expand       | 1            |                                  |
      | 1  | Start        |              |                                  |
      | 6  | Project      | 5            |                                  |
      | 5  | GetVertices  | 4            |                                  |
      | 4  | Argument     |              |                                  |
    When profiling query:
      """
      LOOKUP ON player WHERE player.name=='Tim Duncan' YIELD id(vertex) as id
      | YIELD $-.id AS vid
      |  GO FROM $-.vid OVER like BIDIRECT
      WHERE any(x in split($$.player.name, ' ') WHERE x contains 'Ti')
      YIELD $$.player.name, like._dst AS vid
      | GO FROM $-.vid OVER like BIDIRECT WHERE any(x in split($$.player.name, ' ') WHERE x contains 'Ti')
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name |
      | "Tim Duncan"   |
    And the execution plan should be:
      | id | name               | dependencies | operator info |
      | 26 | Project            | 34           |               |
      | 34 | HashInnerJoin      | 15,33        |               |
      | 15 | Project            | 31           |               |
      | 31 | HashInnerJoin      | 28,30        |               |
      | 28 | Project            | 27           |               |
      | 27 | TagIndexPrefixScan | 0            |               |
      | 0  | Start              |              |               |
      | 30 | Filter             | 29           |               |
      | 29 | HashLeftJoin       | 8,11         |               |
      | 8  | ExpandAll          | 7            |               |
      | 7  | Expand             | 6            |               |
      | 6  | Argument           |              |               |
      | 11 | Project            | 10           |               |
      | 10 | GetVertices        | 9            |               |
      | 9  | Argument           |              |               |
      | 33 | Filter             | 32           |               |
      | 32 | HashLeftJoin       | 19,22        |               |
      | 19 | ExpandAll          | 18           |               |
      | 18 | Expand             | 17           |               |
      | 17 | Argument           |              |               |
      | 22 | Project            | 21           |               |
      | 21 | GetVertices        | 20           |               |
      | 20 | Argument           |              |               |

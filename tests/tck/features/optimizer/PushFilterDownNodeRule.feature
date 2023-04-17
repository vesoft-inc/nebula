# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down node rule
  Examples:
    | space_name  |
    | nba         |
    | nba_int_vid |

  Background: Prepare space
    Given a graph with space named "<space_name>"

  Scenario Outline: push vFilter down to AppendVertices
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'}) return v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                    |
      | 5  | Project        | 6            |                                                                  |
      | 6  | AppendVertices | 7            | {"vertex_filter": "", "filter": "(player.name==\"Tim Duncan\")"} |
      | 7  | IndexScan      | 0            |                                                                  |
      | 0  | Start          |              |                                                                  |
    When profiling query:
      """
      MATCH (v:player)-[]->(m) WHERE m.player.name =~ "T.*" RETURN distinct v.player.name AS vname, m.player.name AS nname
      """
    Then the result should be, in any order:
      | vname               | nname           |
      | "Boris Diaw"        | "Tim Duncan"    |
      | "Boris Diaw"        | "Tony Parker"   |
      | "Dejounte Murray"   | "Tim Duncan"    |
      | "Dejounte Murray"   | "Tony Parker"   |
      | "Manu Ginobili"     | "Tim Duncan"    |
      | "Manu Ginobili"     | "Tony Parker"   |
      | "Grant Hill"        | "Tracy McGrady" |
      | "Tony Parker"       | "Tim Duncan"    |
      | "Vince Carter"      | "Tracy McGrady" |
      | "Shaquille O'Neal"  | "Tim Duncan"    |
      | "LaMarcus Aldridge" | "Tim Duncan"    |
      | "LaMarcus Aldridge" | "Tony Parker"   |
      | "Tim Duncan"        | "Tony Parker"   |
      | "Aron Baynes"       | "Tim Duncan"    |
      | "Danny Green"       | "Tim Duncan"    |
      | "Yao Ming"          | "Tracy McGrady" |
      | "Marco Belinelli"   | "Tim Duncan"    |
      | "Marco Belinelli"   | "Tony Parker"   |
      | "Tiago Splitter"    | "Tim Duncan"    |
    And the execution plan should be:
      | id | name           | dependencies | operator info                        |
      | 8  | Dedup          | 13           |                                      |
      | 13 | Project        | 12           |                                      |
      | 12 | AppendVertices | 11           | {"filter": "(player.name=~\"T.*\")"} |
      | 11 | Traverse       | 1            |                                      |
      | 1  | IndexScan      | 2            |                                      |
      | 2  | Start          |              |                                      |
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'}) WHERE v.player.age == 42 return v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                      |
      | 5  | Project        | 6            |                                                                    |
      | 6  | AppendVertices | 7            | {"filter": "((player.age==42) AND (player.name==\"Tim Duncan\"))"} |
      | 7  | IndexScan      | 0            |                                                                    |
      | 0  | Start          |              |                                                                    |

  Scenario Outline: push vFilter down to Traverse
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like]->() return v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                               |
      | 5  | Project        | 9            |                                                                             |
      | 9  | AppendVertices | 8            |                                                                             |
      | 8  | Traverse       | 7            | {"vertex filter": "", "first step filter": "(player.name==\"Tim Duncan\")"} |
      | 7  | IndexScan      | 0            |                                                                             |
      | 0  | Start          |              |                                                                             |
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like]->() WHERE v.player.age == 42 return v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                               |
      | 5  | Project        | 9            |                                                                             |
      | 9  | Filter         | 6            |                                                                             |
      | 6  | AppendVertices | 8            |                                                                             |
      | 8  | Traverse       | 7            | {"vertex filter": "", "first step filter": "(player.name==\"Tim Duncan\")"} |
      | 7  | IndexScan      | 0            |                                                                             |
      | 0  | Start          |              |                                                                             |

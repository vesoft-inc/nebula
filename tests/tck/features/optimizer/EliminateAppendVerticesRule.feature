# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Eliminate AppendVertices rule

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: eliminate AppendVertices
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like{likeness: 95}]->() return v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
      | "Tim Duncan" |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 5  | Project   | 8            |               |
      | 8  | Traverse  | 7            |               |
      | 7  | IndexScan | 0            |               |
      | 0  | Start     |              |               |

  Scenario: eliminate AppendVertices failed with returned path
    When profiling query:
      """
      MATCH p = (v:player{name: 'Tim Duncan'})-[e:like{likeness: 95}]->() return p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                             |
      | <("Tim Duncan")-[:like@0]->("Tony Parker")>   |
      | <("Tim Duncan")-[:like@0]->("Manu Ginobili")> |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 5  | Project        | 9            |               |
      | 9  | AppendVertices | 8            |               |
      | 8  | Traverse       | 7            |               |
      | 7  | IndexScan      | 0            |               |
      | 0  | Start          |              |               |

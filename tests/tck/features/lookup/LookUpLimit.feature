# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Limit down IndexScan Rule

  Background:
    Given a graph with space named "nba"

  Scenario: push limit down to IndexScan
    When profiling query:
      """
      LOOKUP ON player YIELD id(vertex) as id | Limit 2 | ORDER BY $-.id
      """
    Then the result should be, in any order:
      | id            |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 5  | Sort             | 6            |                |
      | 6  | Project          | 7            |                |
      | 7  | Limit            | 8            | {"count": "2"} |
      | 8  | TagIndexFullScan | 0            | {"limit": "2"} |
      | 0  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON like YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank | Limit 2 | ORDER BY $-.src
      """
    Then the result should be, in any order:
      | src           | dst           | rank  |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 5  | Sort              | 6            |                |
      | 6  | Project           | 7            |                |
      | 7  | Limit             | 8            | {"count": "2"} |
      | 8  | EdgeIndexFullScan | 0            | {"limit": "2"} |
      | 0  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 YIELD id(vertex) as id | Limit 2 | ORDER BY $-.id
      """
    Then the result should be, in any order:
      | id            |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 5  | Sort               | 6            |                |
      | 6  | Project            | 7            |                |
      | 7  | Limit              | 8            | {"count": "2"} |
      | 8  | TagIndexPrefixScan | 0            | {"limit": "2"} |
      | 0  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank | Limit 2 | ORDER BY $-.src
      """
    Then the result should be, in any order:
      | src           | dst           | rank  |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 5  | Sort                | 6            |                |
      | 6  | Project             | 7            |                |
      | 7  | Limit               | 8            | {"count": "2"} |
      | 8  | EdgeIndexPrefixScan | 0            | {"limit": "2"} |
      | 0  | Start               |              |                |

  Scenario: push limit down to IndexScan with limit
    When profiling query:
      """
      LOOKUP ON player YIELD id(vertex) as id | LIMIT 3 | ORDER BY $-.id
      """
    Then the result should be, in any order:
      | id            |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 4  | Sort             | 5            |                |
      | 5  | Project          | 7            |                |
      | 7  | Limit            | 8            |                |
      | 8  | TagIndexFullScan | 9            | {"limit": "3"} |
      | 9  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON like YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank | LIMIT 3 | ORDER BY $-.src
      """
    Then the result should be, in any order:
      | src           | dst           | rank  |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 4  | Sort              | 5            |                |
      | 5  | Project           | 7            |                |
      | 7  | Limit             | 8            |                |
      | 8  | EdgeIndexFullScan | 9            | {"limit": "3"} |
      | 9  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 YIELD id(vertex) as id | LIMIT 3 | ORDER BY $-.id
      """
    Then the result should be, in any order:
      | id            |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 4  | Sort               | 5            |                |
      | 5  | Project            | 7            |                |
      | 7  | Limit              | 8            |                |
      | 8  | TagIndexPrefixScan | 9            | {"limit": "3"} |
      | 9  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank | LIMIT 3 | ORDER BY $-.src
      """
    Then the result should be, in any order:
      | src           | dst           | rank  |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/ |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 4  | Sort                | 5            |                |
      | 5  | Project             | 7            |                |
      | 7  | Limit               | 8            |                |
      | 8  | EdgeIndexPrefixScan | 9            | {"limit": "3"} |
      | 9  | Start               |              |                |

  Scenario: some variables in where clause of lookup statement
    When executing query:
      """
      $var = YIELD true;
      LOOKUP ON player WHERE $var==true YIELD id(vertex) as id;
      """
    Then a SemanticError should be raised at runtime: Expression ($var==true) not supported yet
    When executing query:
      """
      $var = YIELD true;
      LOOKUP ON player WHERE player.name=='Tim Duncan' AND $var==true YIELD id(vertex) as id;
      """
    Then a SemanticError should be raised at runtime: Expression ($var==true) not supported yet
    When executing query:
      """
      YIELD 'Tim Duncan' AS player_name |
      LOOKUP ON player WHERE player.name==$-.player_name YIELD id(vertex) as id;
      """
    Then a SemanticError should be raised at runtime: '$-.player_name' is not an evaluable expression
    When executing query:
      """
      YIELD 'Tim Duncan' AS player_name |
      LOOKUP ON player WHERE player.name IN [$-.player_name, 'Tony Parker'] YIELD id(vertex) as id;
      """
    Then a SemanticError should be raised at runtime: '[$-.player_name,"Tony Parker"]' is not an evaluable expression
    When executing query:
      """
      YIELD 'Tim Duncan' AS player_name |
      LOOKUP ON player WHERE player.name==player_name YIELD id(vertex) as id;
      """
    Then a SemanticError should be raised at runtime: 'player_name' is not an evaluable expression

  Scenario: some variables in yield clause of lookup statement
    When executing query:
      """
      $var = YIELD true;
      LOOKUP ON player WHERE player.name=='Tim Duncan' YIELD id(vertex) as id, $var AS v;
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `$var'
    When executing query:
      """
      YIELD 'Tim Duncan' AS player_name |
      LOOKUP ON player WHERE player.name=='Tim Duncan' YIELD id(vertex) as id, $-.player_name AS pn;
      """
    Then a SemanticError should be raised at runtime: unsupported input/variable property expression in yield
    When executing query:
      """
      YIELD 'Tim Duncan' AS player_name |
      LOOKUP ON player WHERE player.name=='Tim Duncan' YIELD id(vertex) as id, player_name AS pn;
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: player_name

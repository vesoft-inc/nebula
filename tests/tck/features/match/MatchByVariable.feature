# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Match By Variable

  Background:
    Given a graph with space named "nba"

  Scenario: [1] match by vids from with
    When profiling query:
      """
      with ['Tim Duncan', 'Yao Ming'] as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n  |
      | 20 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 9  | Aggregate      | 8            |                |               |
      | 8  | Filter         | 7            |                |               |
      | 7  | CrossJoin      | 1,6          |                |               |
      | 1  | Project        | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 6  | Project        | 11           |                |               |
      | 11 | AppendVertices | 10           |                |               |
      | 10 | Traverse       | 3            |                |               |
      | 3  | Argument       |              |                |               |
    When profiling query:
      """
      with ['Tim Duncan', 'Yao Ming'] as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND id(v2) in ['Tony Parker']
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 4 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 11 | Aggregate      | 15           |                |               |
      | 15 | Filter         | 14           |                |               |
      | 14 | CrossJoin      | 1,17         |                |               |
      | 1  | Project        | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 17 | Project        | 21           |                |               |
      | 21 | AppendVertices | 20           |                |               |
      | 20 | Filter         | 18           |                |               |
      | 18 | Traverse       | 4            |                |               |
      | 4  | Dedup          | 3            |                |               |
      | 3  | PassThrough    | 5            |                |               |
      | 5  | Start          |              |                |               |
    When profiling query:
      """
      with ['Tim Duncan', 'Yao Ming'] as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND id(v2) in id_list
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 9  | Aggregate      | 8            |                |               |
      | 8  | Filter         | 7            |                |               |
      | 7  | CrossJoin      | 1,6          |                |               |
      | 1  | Project        | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 6  | Project        | 11           |                |               |
      | 11 | AppendVertices | 10           |                |               |
      | 10 | Traverse       | 3            |                |               |
      | 3  | Argument       |              |                |               |
    When profiling query:
      """
      with ['Tim Duncan', 'Yao Ming'] as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND v1.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 10 | Aggregate      | 15           |                |               |
      | 15 | Filter         | 14           |                |               |
      | 14 | CrossJoin      | 1,17         |                |               |
      | 1  | Project        | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 17 | Project        | 16           |                |               |
      | 16 | Filter         | 19           |                |               |
      | 19 | AppendVertices | 18           |                |               |
      | 18 | Traverse       | 11           |                |               |
      | 11 | IndexScan      | 4            |                |               |
      | 4  | Start          |              |                |               |
    When profiling query:
      """
      with ['Tim Duncan', 'Yao Ming'] as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND v2.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 4 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 10 | Aggregate      | 15           |                |               |
      | 15 | Filter         | 14           |                |               |
      | 14 | CrossJoin      | 1,17         |                |               |
      | 1  | Project        | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 17 | Project        | 16           |                |               |
      | 16 | Filter         | 19           |                |               |
      | 19 | AppendVertices | 18           |                |               |
      | 18 | Traverse       | 11           |                |               |
      | 11 | IndexScan      | 4            |                |               |
      | 4  | Start          |              |                |               |

  Scenario: [2] match by vids from with
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($v1) IN $id_list). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 13 | Aggregate      | 12           |                |               |
      | 12 | Filter         | 11           |                |               |
      | 11 | CrossJoin      | 6,10         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 16           |                |               |
      | 16 | AppendVertices | 14           |                |               |
      | 14 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 10 | Project        | 18           |                |               |
      | 18 | AppendVertices | 17           |                |               |
      | 17 | Traverse       | 7            |                |               |
      | 7  | Argument       |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list and id(v2) in ['Tony Parker']
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($v1) IN $id_list). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 15 | Aggregate      | 20           |                |               |
      | 20 | Filter         | 19           |                |               |
      | 19 | CrossJoin      | 6,23         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 24           |                |               |
      | 24 | AppendVertices | 16           |                |               |
      | 16 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 23 | Project        | 28           |                |               |
      | 28 | AppendVertices | 27           |                |               |
      | 27 | Filter         | 25           |                |               |
      | 25 | Traverse       | 8            |                |               |
      | 8  | Dedup          | 7            |                |               |
      | 7  | PassThrough    | 9            |                |               |
      | 9  | Start          |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND id(v2) in id_list
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: ((id($v1) IN $id_list) AND (id($v2) IN $id_list)). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 13 | Aggregate      | 12           |                |               |
      | 12 | Filter         | 11           |                |               |
      | 11 | CrossJoin      | 6,10         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 16           |                |               |
      | 16 | AppendVertices | 14           |                |               |
      | 14 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 10 | Project        | 18           |                |               |
      | 18 | AppendVertices | 17           |                |               |
      | 17 | Traverse       | 7            |                |               |
      | 7  | Argument       |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND v1.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($v1) IN $id_list). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 14 | Aggregate      | 20           |                |               |
      | 20 | Filter         | 19           |                |               |
      | 19 | CrossJoin      | 6,23         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 24           |                |               |
      | 24 | AppendVertices | 15           |                |               |
      | 15 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 23 | Project        | 22           |                |               |
      | 22 | Filter         | 26           |                |               |
      | 26 | AppendVertices | 25           |                |               |
      | 25 | Traverse       | 16           |                |               |
      | 16 | IndexScan      | 8            |                |               |
      | 8  | Start          |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND v2.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($v1) IN $id_list). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 14 | Aggregate      | 20           |                |               |
      | 20 | Filter         | 19           |                |               |
      | 19 | CrossJoin      | 6,23         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 24           |                |               |
      | 24 | AppendVertices | 15           |                |               |
      | 15 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 23 | Project        | 22           |                |               |
      | 22 | Filter         | 26           |                |               |
      | 26 | AppendVertices | 25           |                |               |
      | 25 | Traverse       | 16           |                |               |
      | 16 | IndexScan      | 8            |                |               |
      | 8  | Start          |              |                |               |

  Scenario: [3] match by vids from with
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($v1) IN $id_list). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 13 | Aggregate      | 12           |                |               |
      | 12 | Filter         | 11           |                |               |
      | 11 | CrossJoin      | 6,10         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 15           |                |               |
      | 15 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 10 | Project        | 17           |                |               |
      | 17 | AppendVertices | 16           |                |               |
      | 16 | Traverse       | 7            |                |               |
      | 7  | Argument       |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list and id(v2) in ['Tim Duncan', 'Yao Ming']
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($v1) IN $id_list). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 15 | Aggregate      | 19           |                |               |
      | 19 | Filter         | 18           |                |               |
      | 18 | CrossJoin      | 6,22         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 23           |                |               |
      | 23 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 22 | Project        | 27           |                |               |
      | 27 | AppendVertices | 26           |                |               |
      | 26 | Filter         | 24           |                |               |
      | 24 | Traverse       | 8            |                |               |
      | 8  | Dedup          | 7            |                |               |
      | 7  | PassThrough    | 9            |                |               |
      | 9  | Start          |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND id(v2) in id_list
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: ((id($v1) IN $id_list) AND (id($v2) IN $id_list)). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 13 | Aggregate      | 12           |                |               |
      | 12 | Filter         | 11           |                |               |
      | 11 | CrossJoin      | 6,10         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 15           |                |               |
      | 15 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 10 | Project        | 17           |                |               |
      | 17 | AppendVertices | 16           |                |               |
      | 16 | Traverse       | 7            |                |               |
      | 7  | Argument       |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND v1.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($v1) IN $id_list). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 14 | Aggregate      | 19           |                |               |
      | 19 | Filter         | 18           |                |               |
      | 18 | CrossJoin      | 6,22         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 23           |                |               |
      | 23 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 22 | Project        | 21           |                |               |
      | 21 | Filter         | 25           |                |               |
      | 25 | AppendVertices | 24           |                |               |
      | 24 | Traverse       | 15           |                |               |
      | 15 | IndexScan      | 8            |                |               |
      | 8  | Start          |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as id_list
      match (v1:player)-[e]-(v2:player)
      where id(v1) in id_list AND v2.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($v1) IN $id_list). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 14 | Aggregate      | 19           |                |               |
      | 19 | Filter         | 18           |                |               |
      | 18 | CrossJoin      | 6,22         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 23           |                |               |
      | 23 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 22 | Project        | 21           |                |               |
      | 21 | Filter         | 25           |                |               |
      | 25 | AppendVertices | 24           |                |               |
      | 24 | Traverse       | 15           |                |               |
      | 15 | IndexScan      | 8            |                |               |
      | 8  | Start          |              |                |               |

  Scenario: [1] match by vids from unwind
    When profiling query:
      """
      unwind ['Tim Duncan', 'Yao Ming'] as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n  |
      | 20 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 9  | Aggregate      | 8            |                |               |
      | 8  | Filter         | 7            |                |               |
      | 7  | CrossJoin      | 1,6          |                |               |
      | 1  | Unwind         | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 6  | Project        | 11           |                |               |
      | 11 | AppendVertices | 10           |                |               |
      | 10 | Traverse       | 3            |                |               |
      | 3  | Argument       |              |                |               |
    When profiling query:
      """
      unwind ['Tim Duncan', 'Yao Ming'] as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid and id(v2)=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 4 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 11 | Aggregate      | 15           |                |               |
      | 15 | Filter         | 14           |                |               |
      | 14 | CrossJoin      | 1,17         |                |               |
      | 1  | Unwind         | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 17 | Project        | 21           |                |               |
      | 21 | AppendVertices | 20           |                |               |
      | 20 | Filter         | 18           |                |               |
      | 18 | Traverse       | 4            |                |               |
      | 4  | Dedup          | 3            |                |               |
      | 3  | PassThrough    | 5            |                |               |
      | 5  | Start          |              |                |               |
    When profiling query:
      """
      unwind ['Tim Duncan', 'Yao Ming'] as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND id(v2)==vid
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 9  | Aggregate      | 8            |                |               |
      | 8  | Filter         | 7            |                |               |
      | 7  | CrossJoin      | 1,6          |                |               |
      | 1  | Unwind         | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 6  | Project        | 11           |                |               |
      | 11 | AppendVertices | 10           |                |               |
      | 10 | Traverse       | 3            |                |               |
      | 3  | Argument       |              |                |               |
    When profiling query:
      """
      unwind ['Tim Duncan', 'Yao Ming'] as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND v1.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 10 | Aggregate      | 15           |                |               |
      | 15 | Filter         | 14           |                |               |
      | 14 | CrossJoin      | 1,17         |                |               |
      | 1  | Unwind         | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 17 | Project        | 16           |                |               |
      | 16 | Filter         | 19           |                |               |
      | 19 | AppendVertices | 18           |                |               |
      | 18 | Traverse       | 11           |                |               |
      | 11 | IndexScan      | 4            |                |               |
      | 4  | Start          |              |                |               |
    When profiling query:
      """
      unwind ['Tim Duncan', 'Yao Ming'] as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND v2.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 4 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 10 | Aggregate      | 15           |                |               |
      | 15 | Filter         | 14           |                |               |
      | 14 | CrossJoin      | 1,17         |                |               |
      | 1  | Unwind         | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 17 | Project        | 16           |                |               |
      | 16 | Filter         | 19           |                |               |
      | 19 | AppendVertices | 18           |                |               |
      | 18 | Traverse       | 11           |                |               |
      | 11 | IndexScan      | 4            |                |               |
      | 4  | Start          |              |                |               |

  Scenario: [2] match by vids from unwind
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1) == vid
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 2 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 13 | Aggregate      | 12           |                |               |
      | 12 | Filter         | 11           |                |               |
      | 11 | CrossJoin      | 6,10         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 16           |                |               |
      | 16 | AppendVertices | 14           |                |               |
      | 14 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 10 | Project        | 18           |                |               |
      | 18 | AppendVertices | 17           |                |               |
      | 17 | Traverse       | 7            |                |               |
      | 7  | Argument       |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1) == vid and id(v2)=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 15 | Aggregate      | 20           |                |               |
      | 20 | Filter         | 19           |                |               |
      | 19 | CrossJoin      | 6,23         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 24           |                |               |
      | 24 | AppendVertices | 16           |                |               |
      | 16 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 23 | Project        | 28           |                |               |
      | 28 | AppendVertices | 27           |                |               |
      | 27 | Filter         | 25           |                |               |
      | 25 | Traverse       | 8            |                |               |
      | 8  | Dedup          | 7            |                |               |
      | 7  | PassThrough    | 9            |                |               |
      | 9  | Start          |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND id(v2)==vid
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 13 | Aggregate      | 12           |                |               |
      | 12 | Filter         | 11           |                |               |
      | 11 | CrossJoin      | 6,10         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 16           |                |               |
      | 16 | AppendVertices | 14           |                |               |
      | 14 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 10 | Project        | 18           |                |               |
      | 18 | AppendVertices | 17           |                |               |
      | 17 | Traverse       | 7            |                |               |
      | 7  | Argument       |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND v1.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 14 | Aggregate      | 20           |                |               |
      | 20 | Filter         | 19           |                |               |
      | 19 | CrossJoin      | 6,23         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 24           |                |               |
      | 24 | AppendVertices | 15           |                |               |
      | 15 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 23 | Project        | 22           |                |               |
      | 22 | Filter         | 26           |                |               |
      | 26 | AppendVertices | 25           |                |               |
      | 25 | Traverse       | 16           |                |               |
      | 16 | IndexScan      | 8            |                |               |
      | 8  | Start          |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name=='Yao Ming'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND v2.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 14 | Aggregate      | 20           |                |               |
      | 20 | Filter         | 19           |                |               |
      | 19 | CrossJoin      | 6,23         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 24           |                |               |
      | 24 | AppendVertices | 15           |                |               |
      | 15 | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 23 | Project        | 22           |                |               |
      | 22 | Filter         | 26           |                |               |
      | 26 | AppendVertices | 25           |                |               |
      | 25 | Traverse       | 16           |                |               |
      | 16 | IndexScan      | 8            |                |               |
      | 8  | Start          |              |                |               |

  Scenario: [3] match by vids from unwind
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n  |
      | 40 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 13 | Aggregate      | 12           |                |               |
      | 12 | Filter         | 11           |                |               |
      | 11 | CrossJoin      | 6,10         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 15           |                |               |
      | 15 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 10 | Project        | 17           |                |               |
      | 17 | AppendVertices | 16           |                |               |
      | 16 | Traverse       | 7            |                |               |
      | 7  | Argument       |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid and id(v2)=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 4 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 15 | Aggregate      | 19           |                |               |
      | 19 | Filter         | 18           |                |               |
      | 18 | CrossJoin      | 6,22         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 23           |                |               |
      | 23 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 22 | Project        | 27           |                |               |
      | 27 | AppendVertices | 26           |                |               |
      | 26 | Filter         | 24           |                |               |
      | 24 | Traverse       | 8            |                |               |
      | 8  | Dedup          | 7            |                |               |
      | 7  | PassThrough    | 9            |                |               |
      | 9  | Start          |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND id(v2)==vid
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 0 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 13 | Aggregate      | 12           |                |               |
      | 12 | Filter         | 11           |                |               |
      | 11 | CrossJoin      | 6,10         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 15           |                |               |
      | 15 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 10 | Project        | 17           |                |               |
      | 17 | AppendVertices | 16           |                |               |
      | 16 | Traverse       | 7            |                |               |
      | 7  | Argument       |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND v1.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n  |
      | 14 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 14 | Aggregate      | 19           |                |               |
      | 19 | Filter         | 18           |                |               |
      | 18 | CrossJoin      | 6,22         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 23           |                |               |
      | 23 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 22 | Project        | 21           |                |               |
      | 21 | Filter         | 25           |                |               |
      | 25 | AppendVertices | 24           |                |               |
      | 24 | Traverse       | 15           |                |               |
      | 15 | IndexScan      | 8            |                |               |
      | 8  | Start          |              |                |               |
    When profiling query:
      """
      match (v:player)
      where v.player.name starts with 'T'
      with id(v) as vid
      match (v1:player)-[e]-(v2:player)
      where id(v1)==vid AND v2.player.name=='Tony Parker'
      return count(*) AS n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
      | 4 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 14 | Aggregate      | 19           |                |               |
      | 19 | Filter         | 18           |                |               |
      | 18 | CrossJoin      | 6,22         |                |               |
      | 6  | Project        | 5            |                |               |
      | 5  | Filter         | 23           |                |               |
      | 23 | AppendVertices | 1            |                |               |
      | 1  | IndexScan      | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 22 | Project        | 21           |                |               |
      | 21 | Filter         | 25           |                |               |
      | 25 | AppendVertices | 24           |                |               |
      | 24 | Traverse       | 15           |                |               |
      | 15 | IndexScan      | 8            |                |               |
      | 8  | Start          |              |                |               |

  Scenario: unwind with and match
    When profiling query:
      """
      unwind [1, 2] AS x
      with ['Tim Duncan', 'Yao Ming'] AS id_list
      match (v1:player)--(v2)
      where id(v1) IN id_list
      return count(*) as n
      """
    Then the result should be, in any order, with relax comparison:
      | n  |
      | 44 |
    And the execution plan should be:
      | id | name           | dependencies | profiling data | operator info |
      | 10 | Aggregate      | 9            |                |               |
      | 9  | Filter         | 8            |                |               |
      | 8  | CrossJoin      | 3,7          |                |               |
      | 3  | Project        | 1            |                |               |
      | 1  | Unwind         | 2            |                |               |
      | 2  | Start          |              |                |               |
      | 7  | Project        | 6            |                |               |
      | 6  | AppendVertices | 11           |                |               |
      | 11 | Traverse       | 4            |                |               |
      | 4  | Argument       |              |                |               |

  Scenario: invalid variable in id condition
    When executing query:
      """
      match (v1:player)--(v2)
      where id(v1) IN v2
      return count(*) as n
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: (id($-.v1) IN $-.v2). For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.

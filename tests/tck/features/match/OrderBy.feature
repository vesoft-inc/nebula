# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: OrderBy in Match

  Background:
    Given a graph with space named "nba"

  Scenario: orderby hidden column
    When executing query:
      """
      match (v:player)-->(v1)
      with v, v1, count(v1) as a0
      where a0 > 1
      return sum(v.player.age)
      order by a0
      limit 5
      """
    Then a SemanticError should be raised at runtime: Column `a0' not found
    When executing query:
      """
      match (v:player)-->(v1)
      with v, v1, count(v1) as a0
      where a0 > 1
      return distinct v
      order by a0
      limit 5
      """
    Then a SemanticError should be raised at runtime: Column `a0' not found
    When executing query:
      """
      match (v:player)-->(v1:player)
      with v, v1, v.player.name as name, v1.player.name as name2, count(v) as a0
      where a0 > 1
      return v, v1
      order by a0, name, name2
      """
    Then the result should be, in order:
      | v                                                                                                           | v1                                                                                                          |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) |

  Scenario: topn hidden column
    When executing query:
      """
      match (v:player)-->(v1:player)
      with v, v1, v.player.name as name, v1.player.name as name2, count(v) as a0
      where a0 > 0
      return name
      order by a0 desc, name desc, name2 desc
      limit 10
      """
    Then the result should be, in order:
      | name            |
      | "Tony Parker"   |
      | "Tony Parker"   |
      | "Tony Parker"   |
      | "Tim Duncan"    |
      | "Tim Duncan"    |
      | "Manu Ginobili" |
      | "Yao Ming"      |
      | "Yao Ming"      |
      | "Vince Carter"  |
      | "Vince Carter"  |
    When executing query:
      """
      match (v:player)-->(v1:player)
      with v, v1, v.player.name as name, v1.player.name as name2, count(v) as a0
      where a0 > 0
      return a0, name
      order by a0 desc, name desc, name2 desc
      limit 10
      """
    Then the result should be, in order:
      | a0 | name            |
      | 2  | "Tony Parker"   |
      | 2  | "Tony Parker"   |
      | 2  | "Tony Parker"   |
      | 2  | "Tim Duncan"    |
      | 2  | "Tim Duncan"    |
      | 2  | "Manu Ginobili" |
      | 1  | "Yao Ming"      |
      | 1  | "Yao Ming"      |
      | 1  | "Vince Carter"  |
      | 1  | "Vince Carter"  |

  Scenario: orderby return column
    When executing query:
      """
      match (v:player)-->(v1:player)
      with v, v1, v.player.age as age0, v1.player.age as age1, count(v) as a0
      where a0 > 1
      return age0, age1
      order by age0, age1
      """
    Then the result should be, in order:
      | age0 | age1 |
      | 36   | 33   |
      | 36   | 41   |
      | 36   | 42   |
      | 41   | 42   |
      | 42   | 36   |
      | 42   | 41   |
    When executing query:
      """
      match (v:player)-->(v1:player)
      with v, v1, v.player.name as name, v1.player.name as name2, count(v) as a0
      where a0 > 1
      return name, a0
      order by a0, name desc, name2 desc
      """
    Then the result should be, in order:
      | name            | a0 |
      | "Tony Parker"   | 2  |
      | "Tony Parker"   | 2  |
      | "Tony Parker"   | 2  |
      | "Tim Duncan"    | 2  |
      | "Tim Duncan"    | 2  |
      | "Manu Ginobili" | 2  |
    When executing query:
      """
      match (v:player)-->(v1:player)
      with v.player.name as name, v1, v1.player.name as name2, count(v) as a0
      where a0 > 1
      return *
      order by name, a0 desc, name2 desc
      """
    Then the result should be, in order:
      | name            | v1                                                                                                          | name2               | a0 |
      | "Manu Ginobili" | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | "Tim Duncan"        | 2  |
      | "Tim Duncan"    | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | "Tony Parker"       | 2  |
      | "Tim Duncan"    | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | "Manu Ginobili"     | 2  |
      | "Tony Parker"   | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | "Tim Duncan"        | 2  |
      | "Tony Parker"   | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | "Manu Ginobili"     | 2  |
      | "Tony Parker"   | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | "LaMarcus Aldridge" | 2  |

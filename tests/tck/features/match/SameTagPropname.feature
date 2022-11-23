# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Same Tag Propname

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(32) |

  Scenario: same tag propname
    Given having executed:
      """
      CREATE TAG student(name string, age int, score float);
      CREATE TAG player(name string, age int, height float);
      CREATE EDGE like(likeness int);
      CREATE TAG INDEX s_age_index ON student(age);
      CREATE TAG INDEX s_name_index ON student(name(20));
      CREATE TAG INDEX s_score_index ON student(score);
      CREATE TAG INDEX p_name_index ON player(name(20));
      CREATE TAG INDEX p_age_index ON player(age);
      CREATE TAG INDEX p_height_index ON player(height);
      CREATE EDGE INDEX like_likeness_index ON like(likeness);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX
        student(name, age, score)
      VALUES
        "zhang":("s_zhang", 18, 89),
        "wang": ("s_wang",  22, 82),
        "li":   ("s_li",    20, 91),
        "zhao": ("s_zhao",  20, 99),
        "qian": ("s_qian",  19, 88),
        "sun":  ("s_sun",   17, 72 );
      INSERT VERTEX
        player(name, age, height)
      VALUES
        "zhang":("p_zhang", 18, 189),
        "wang": ("p_wang",  22, 192),
        "li":   ("p_li",    20, 201),
        "zhao": ("p_zhao",  20, 187),
        "qian": ("p_qian",  19, 179),
        "sun":  ("p_sun",   17, 192);
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT EDGE
        like(likeness)
      VALUES
        "zhang"->"wang":(98),
        "zhang"->"li":(89),
        "zhang"->"zhao":(99),
        "zhang"->"sun":(78),
        "zhao"->"sun":(29),
        "zhao"->"wang":(89),
        "zhao"->"li":(99),
        "zhao"->"qian":(100),
        "li"->"sun":(99),
        "li"->"qian":(89),
        "qian"->"zhang":(20),
        "qian"->"wang":(79),
        "sun"->"li":(93),
        "sun"->"wang":(95),
        "wang"->"li":(74),
        "wang"->"zhang":(91);
      """
    Then the execution should be successful
    When executing query:
      """
      match (v:player) return v.player.name, v
      """
    Then the result should be, in any order:
      | v.player.name | v                                                                                                          |
      | "p_qian"      | ("qian" :player{age: 19, height: 179.0, name: "p_qian"} :student{age: 19, name: "s_qian", score: 88.0})    |
      | "p_zhao"      | ("zhao" :player{age: 20, height: 187.0, name: "p_zhao"} :student{age: 20, name: "s_zhao", score: 99.0})    |
      | "p_wang"      | ("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})    |
      | "p_li"        | ("li" :player{age: 20, height: 201.0, name: "p_li"} :student{age: 20, name: "s_li", score: 91.0})          |
      | "p_zhang"     | ("zhang" :player{age: 18, height: 189.0, name: "p_zhang"} :student{age: 18, name: "s_zhang", score: 89.0}) |
      | "p_sun"       | ("sun" :player{age: 17, height: 192.0, name: "p_sun"} :student{age: 17, name: "s_sun", score: 72.0})       |
    When executing query:
      """
      match (v:player) where v.player.age > 20 return v.player.name, v
      """
    Then the result should be, in any order:
      | v.player.name | v                                                                                                       |
      | "p_wang"      | ("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0}) |
    When executing query:
      """
      match (v:student)  return v.student.name, v
      """
    Then the result should be, in any order:
      | v.student.name | v                                                                                                          |
      | "s_qian"       | ("qian" :player{age: 19, height: 179.0, name: "p_qian"} :student{age: 19, name: "s_qian", score: 88.0})    |
      | "s_zhao"       | ("zhao" :player{age: 20, height: 187.0, name: "p_zhao"} :student{age: 20, name: "s_zhao", score: 99.0})    |
      | "s_wang"       | ("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})    |
      | "s_li"         | ("li" :player{age: 20, height: 201.0, name: "p_li"} :student{age: 20, name: "s_li", score: 91.0})          |
      | "s_zhang"      | ("zhang" :player{age: 18, height: 189.0, name: "p_zhang"} :student{age: 18, name: "s_zhang", score: 89.0}) |
      | "s_sun"        | ("sun" :player{age: 17, height: 192.0, name: "p_sun"} :student{age: 17, name: "s_sun", score: 72.0})       |
    When executing query:
      """
      match (v:student) where v.student.age > 20 return v.student.name, v
      """
    Then the result should be, in any order:
      | v.student.name | v                                                                                                       |
      | "s_wang"       | ("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0}) |
    When executing query:
      """
      match (v:player)-[e:like]->(d) where v.player.age > 19 return e, v.student.name, v.player.name
      """
    Then the result should be, in any order:
      | e                                         | v.student.name | v.player.name |
      | [:like "li"->"sun" @0 {likeness: 99}]     | "s_li"         | "p_li"        |
      | [:like "li"->"qian" @0 {likeness: 89}]    | "s_li"         | "p_li"        |
      | [:like "zhao"->"li" @0 {likeness: 99}]    | "s_zhao"       | "p_zhao"      |
      | [:like "zhao"->"qian" @0 {likeness: 100}] | "s_zhao"       | "p_zhao"      |
      | [:like "zhao"->"sun" @0 {likeness: 29}]   | "s_zhao"       | "p_zhao"      |
      | [:like "zhao"->"wang" @0 {likeness: 89}]  | "s_zhao"       | "p_zhao"      |
      | [:like "wang"->"li" @0 {likeness: 74}]    | "s_wang"       | "p_wang"      |
      | [:like "wang"->"zhang" @0 {likeness: 91}] | "s_wang"       | "p_wang"      |
    When executing query:
      """
      match (v:player)-[e:like]->(d) where e.likeness  > 85 return e, v.student.name, v.player.name
      """
    Then the result should be, in any order:
      | e                                         | v.student.name | v.player.name |
      | [:like "zhang"->"li" @0 {likeness: 89}]   | "s_zhang"      | "p_zhang"     |
      | [:like "li"->"sun" @0 {likeness: 99}]     | "s_li"         | "p_li"        |
      | [:like "zhang"->"wang" @0 {likeness: 98}] | "s_zhang"      | "p_zhang"     |
      | [:like "zhang"->"zhao" @0 {likeness: 99}] | "s_zhang"      | "p_zhang"     |
      | [:like "sun"->"li" @0 {likeness: 93}]     | "s_sun"        | "p_sun"       |
      | [:like "sun"->"wang" @0 {likeness: 95}]   | "s_sun"        | "p_sun"       |
      | [:like "li"->"qian" @0 {likeness: 89}]    | "s_li"         | "p_li"        |
      | [:like "wang"->"zhang" @0 {likeness: 91}] | "s_wang"       | "p_wang"      |
      | [:like "zhao"->"li" @0 {likeness: 99}]    | "s_zhao"       | "p_zhao"      |
      | [:like "zhao"->"qian" @0 {likeness: 100}] | "s_zhao"       | "p_zhao"      |
      | [:like "zhao"->"wang" @0 {likeness: 89}]  | "s_zhao"       | "p_zhao"      |
    When executing query:
      """
      match (v:player) where v.student.score > 80 return v.student.name, v.player.height, v
      """
    Then the result should be, in any order:
      | v.student.name | v.player.height | v                                                                                                          |
      | "s_qian"       | 179.0           | ("qian" :player{age: 19, height: 179.0, name: "p_qian"} :student{age: 19, name: "s_qian", score: 88.0})    |
      | "s_zhao"       | 187.0           | ("zhao" :player{age: 20, height: 187.0, name: "p_zhao"} :student{age: 20, name: "s_zhao", score: 99.0})    |
      | "s_wang"       | 192.0           | ("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})    |
      | "s_li"         | 201.0           | ("li" :player{age: 20, height: 201.0, name: "p_li"} :student{age: 20, name: "s_li", score: 91.0})          |
      | "s_zhang"      | 189.0           | ("zhang" :player{age: 18, height: 189.0, name: "p_zhang"} :student{age: 18, name: "s_zhang", score: 89.0}) |
    When executing query:
      """
      match (v:student {age:20}) where v.player.height > 190 return v
      """
    Then the result should be, in any order:
      | v                                                                                                 |
      | ("li" :player{age: 20, height: 201.0, name: "p_li"} :student{age: 20, name: "s_li", score: 91.0}) |
    When executing query:
      """
      match p= (v:player)-[e:like*1..2]->(d) where v.player.age > 20 return p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                                                                                                                                                                                                                |
      | <("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})-[:like@0 {likeness: 91}]->("zhang" :player{age: 18, height: 189.0, name: "p_zhang"} :student{age: 18, name: "s_zhang", score: 89.0})-[:like@0 {likeness: 99}]->("zhao" :player{age: 20, height: 187.0, name: "p_zhao"} :student{age: 20, name: "s_zhao", score: 99.0})> |
      | <("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})-[:like@0 {likeness: 91}]->("zhang" :player{age: 18, height: 189.0, name: "p_zhang"} :student{age: 18, name: "s_zhang", score: 89.0})-[:like@0 {likeness: 98}]->("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})> |
      | <("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})-[:like@0 {likeness: 91}]->("zhang" :player{age: 18, height: 189.0, name: "p_zhang"} :student{age: 18, name: "s_zhang", score: 89.0})-[:like@0 {likeness: 78}]->("sun" :player{age: 17, height: 192.0, name: "p_sun"} :student{age: 17, name: "s_sun", score: 72.0})>    |
      | <("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})-[:like@0 {likeness: 91}]->("zhang" :player{age: 18, height: 189.0, name: "p_zhang"} :student{age: 18, name: "s_zhang", score: 89.0})-[:like@0 {likeness: 89}]->("li" :player{age: 20, height: 201.0, name: "p_li"} :student{age: 20, name: "s_li", score: 91.0})>       |
      | <("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})-[:like@0 {likeness: 74}]->("li" :player{age: 20, height: 201.0, name: "p_li"} :student{age: 20, name: "s_li", score: 91.0})-[:like@0 {likeness: 99}]->("sun" :player{age: 17, height: 192.0, name: "p_sun"} :student{age: 17, name: "s_sun", score: 72.0})>             |
      | <("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})-[:like@0 {likeness: 74}]->("li" :player{age: 20, height: 201.0, name: "p_li"} :student{age: 20, name: "s_li", score: 91.0})-[:like@0 {likeness: 89}]->("qian" :player{age: 19, height: 179.0, name: "p_qian"} :student{age: 19, name: "s_qian", score: 88.0})>          |
      | <("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})-[:like@0 {likeness: 74}]->("li" :player{age: 20, height: 201.0, name: "p_li"} :student{age: 20, name: "s_li", score: 91.0})>                                                                                                                                            |
      | <("wang" :player{age: 22, height: 192.0, name: "p_wang"} :student{age: 22, name: "s_wang", score: 82.0})-[:like@0 {likeness: 91}]->("zhang" :player{age: 18, height: 189.0, name: "p_zhang"} :student{age: 18, name: "s_zhang", score: 89.0})>                                                                                                                                   |
    When executing query:
      """
      match p= (v:player)-[e:like*1..2]->(d) where v.player.age > 20 return e
      """
    Then the result should be, in any order:
      | e                                                                                      |
      | [[:like "wang"->"zhang" @0 {likeness: 91}], [:like "zhang"->"zhao" @0 {likeness: 99}]] |
      | [[:like "wang"->"zhang" @0 {likeness: 91}], [:like "zhang"->"wang" @0 {likeness: 98}]] |
      | [[:like "wang"->"zhang" @0 {likeness: 91}], [:like "zhang"->"sun" @0 {likeness: 78}]]  |
      | [[:like "wang"->"zhang" @0 {likeness: 91}], [:like "zhang"->"li" @0 {likeness: 89}]]   |
      | [[:like "wang"->"li" @0 {likeness: 74}], [:like "li"->"sun" @0 {likeness: 99}]]        |
      | [[:like "wang"->"li" @0 {likeness: 74}], [:like "li"->"qian" @0 {likeness: 89}]]       |
      | [[:like "wang"->"li" @0 {likeness: 74}]]                                               |
      | [[:like "wang"->"zhang" @0 {likeness: 91}]]                                            |
    When executing query:
      """
      match p= (v:player)-[e:like]->(d) where v.student.age > 19 return v.student.score, v.student.name, v.player.name
      """
    Then the result should be, in any order:
      | v.student.score | v.student.name | v.player.name |
      | 91.0            | "s_li"         | "p_li"        |
      | 91.0            | "s_li"         | "p_li"        |
      | 99.0            | "s_zhao"       | "p_zhao"      |
      | 99.0            | "s_zhao"       | "p_zhao"      |
      | 99.0            | "s_zhao"       | "p_zhao"      |
      | 99.0            | "s_zhao"       | "p_zhao"      |
      | 82.0            | "s_wang"       | "p_wang"      |
      | 82.0            | "s_wang"       | "p_wang"      |
    When executing query:
      """
      match (v:player) where v.student.height > 190 return v
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      match (v:player) where v.abc.height > 190 return v.player.name
      """
    Then the result should be, in any order:
      | v.player.name |
    Then drop the used space

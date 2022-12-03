# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Filter down HashInnerJoin rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down BiInnerJoin
    When profiling query:
      """
      MATCH (v:player)
      WHERE id(v) == 'Tim Duncan'
      WITH v AS v1
      MATCH (v1)-[e:like]-(v2)
      WHERE e.likeness > 0
      RETURN e, v2
      ORDER BY e, v2
      """
    Then the result should be, in any order:
      | e                                                           | v2                                                                |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]   | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]  | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]    | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
    And the execution plan should be:
      | id | name           | dependencies | operator info                           |
      | 30 | Sort           | 14           |                                         |
      | 14 | Project        | 19           |                                         |
      | 19 | BiInnerJoin    | 6,22         |                                         |
      | 6  | Project        | 20           |                                         |
      | 20 | AppendVertices | 2            |                                         |
      | 2  | Dedup          | 1            |                                         |
      | 1  | PassThrough    | 3            |                                         |
      | 3  | Start          |              |                                         |
      | 22 | Project        | 21           |                                         |
      | 21 | Filter         | 10           | { "condition": "($-.e[0].likeness>0)" } |
      | 10 | AppendVertices | 9            |                                         |
      | 9  | Traverse       | 7            |                                         |
      | 7  | Argument       | 8            |                                         |
      | 8  | Start          |              |                                         |
    When profiling query:
      """
      MATCH (v:player)
      WHERE id(v) == 'Tim Duncan'
      WITH v AS v1
      MATCH (v1)-[e:like]-(v2)
      WHERE v1.player.age > 0
      RETURN e, v2
      ORDER BY e, v2
      """
    Then the result should be, in any order:
      | e                                                           | v2                                                                |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]   | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]  | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]    | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
    And the execution plan should be:
      | id | name           | dependencies | operator info                       |
      | 30 | Sort           | 14           |                                     |
      | 14 | Project        | 19           |                                     |
      | 19 | BiInnerJoin    | 6,11         |                                     |
      | 6  | Project        | 16           |                                     |
      | 16 | Filter         | 20           | { "condition": "(v.player.age>0)" } |
      | 20 | AppendVertices | 2            |                                     |
      | 2  | Dedup          | 1            |                                     |
      | 1  | PassThrough    | 3            |                                     |
      | 3  | Start          |              |                                     |
      | 11 | Project        | 10           |                                     |
      | 10 | AppendVertices | 9            |                                     |
      | 9  | Traverse       | 7            |                                     |
      | 7  | Argument       | 8            |                                     |
      | 8  | Start          |              |                                     |
    When profiling query:
      """
      MATCH (v:player)
      WHERE id(v) == 'Tim Duncan'
      WITH v AS v1
      MATCH (v1)-[e:like]-(v2)
      WHERE e.likeness > 0 AND v1.player.age > 0
      RETURN e, v2
      ORDER BY e, v2
      """
    Then the result should be, in any order:
      | e                                                           | v2                                                                |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]   | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]  | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]    | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
    And the execution plan should be:
      | id | name           | dependencies | operator info                         |
      | 30 | Sort           | 14           |                                       |
      | 14 | Project        | 20           |                                       |
      | 20 | BiInnerJoin    | 23,25        |                                       |
      | 23 | Project        | 22           |                                       |
      | 22 | Filter         | 21           | {"condition": "(v.player.age>0)"}     |
      | 21 | AppendVertices | 2            |                                       |
      | 2  | Dedup          | 1            |                                       |
      | 1  | PassThrough    | 3            |                                       |
      | 3  | Start          |              |                                       |
      | 25 | Project        | 24           |                                       |
      | 24 | Filter         | 10           | {"condition": "($-.e[0].likeness>0)"} |
      | 10 | AppendVertices | 9            |                                       |
      | 9  | Traverse       | 7            |                                       |
      | 7  | Argument       | 8            |                                       |
      | 8  | Start          |              |                                       |
    When profiling query:
      """
      MATCH (v:player)
      WHERE id(v) == 'Tim Duncan'
      WITH v AS v1
      MATCH (v1)-[e:like]-(v2)
      WHERE e.likeness > 0 OR v1.player.age > 0
      RETURN e, v2
      ORDER BY e, v2
      """
    Then the result should be, in any order:
      | e                                                           | v2                                                                |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]   | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     |
      | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]  | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]    | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                    |
      | 30 | Sort           | 14           |                                                                  |
      | 14 | Project        | 19           |                                                                  |
      | 19 | BiInnerJoin    | 6,22         |                                                                  |
      | 6  | Project        | 20           |                                                                  |
      | 20 | AppendVertices | 2            |                                                                  |
      | 2  | Dedup          | 1            |                                                                  |
      | 1  | PassThrough    | 3            |                                                                  |
      | 3  | Start          |              |                                                                  |
      | 22 | Project        | 21           |                                                                  |
      | 21 | Filter         | 10           | { "condition": "(($-.e[0].likeness>0) OR (-.v1.player.age>0))" } |
      | 10 | AppendVertices | 9            |                                                                  |
      | 9  | Traverse       | 7            |                                                                  |
      | 7  | Argument       | 8            |                                                                  |
      | 8  | Start          |              |                                                                  |

  Scenario: NOT push filter down BiInnerJoin
    When profiling query:
      """
      MATCH (v:player)-[]-(vv)
      WHERE id(v) == 'Tim Duncan'
      WITH v AS v1, vv AS vv
      MATCH (v1)-[e:like]-(v2)
      WHERE e.likeness > 90 OR vv.team.start_year>2000
      RETURN e, v2
      ORDER BY e, v2
      LIMIT 3
      """
    Then the result should be, in any order:
      | e                                                         | v2                                                            |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}] | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"}) |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}] | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"}) |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}] | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"}) |
    And the execution plan should be:
      | id | name           | dependencies | operator info                                                      |
      | 20 | TopN           | 15           |                                                                    |
      | 15 | Project        | 14           |                                                                    |
      | 14 | Filter         | 13           | { "condition": "(($e.likeness>90) OR (vv.team.start_year>2000))" } |
      | 13 | BiInnerJoin    | 16,12        |                                                                    |
      | 16 | Project        | 5            |                                                                    |
      | 5  | AppendVertices | 17           |                                                                    |
      | 17 | Traverse       | 2            |                                                                    |
      | 2  | Dedup          | 1            |                                                                    |
      | 1  | PassThrough    | 3            |                                                                    |
      | 3  | Start          |              |                                                                    |
      | 12 | Project        | 11           |                                                                    |
      | 11 | AppendVertices | 10           |                                                                    |
      | 10 | Traverse       | 8            |                                                                    |
      | 8  | Argument       | 9            |                                                                    |
      | 9  | Start          |              |                                                                    |

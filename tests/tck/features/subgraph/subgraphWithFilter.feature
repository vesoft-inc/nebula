# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: subgraph with fitler

  Background:
    Given a graph with space named "nba"

  Scenario: subgraph with edge filter
    When executing query:
      """
      GET SUBGRAPH FROM 'Tim Duncan' OUT like WHERE like.likeness > 90 YIELD vertices as v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                    |
      | [("Tim Duncan")]                     |
      | [("Manu Ginobili"), ("Tony Parker")] |
    When executing query:
      """
      GET SUBGRAPH FROM 'Tim Duncan' OUT like WHERE like.likeness > 90 YIELD vertices as v, edges as e
      """
    Then the result should be, in any order, with relax comparison:
      | v                                    | e                                                                                                                 |
      | [("Tim Duncan")]                     | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]  |
      | [("Manu Ginobili"), ("Tony Parker")] | [[:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}], [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]] |
    When executing query:
      """
      GET SUBGRAPH FROM 'Tim Duncan' BOTH like WHERE like.likeness > 90 YIELD vertices as v, edges as e
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                         | e                                                                                                                                                                                                                                 |
      | [("Tim Duncan")]                                          | [[:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}],[:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}], [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]] |
      | [("Dejounte Murray"), ("Manu Ginobili"), ("Tony Parker")] | [[:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}], [:like "Dejounte Murray"->"Tony Parker" @0 {likeness: 99}],[:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]]                                               |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM 'Tim Duncan'
        WHERE like.likeness > 95 AND like.likeness < 100
        YIELD vertices as nodes, edges as relationships
      """
    Then define some list variables:
      | edge2                                            | vertex3               |
      | [:like "Dejounte Murray"->"Chris Paul"@0]        | ("Kyle Anderson")     |
      | [:like "Dejounte Murray"->"Danny Green"@0]       | ("Kevin Durant")      |
      | [:like "Dejounte Murray"->"James Harden"@0]      | ("James Harden")      |
      | [:like "Dejounte Murray"->"Kevin Durant"@0]      | ("Chris Paul")        |
      | [:like "Dejounte Murray"->"Kyle Anderson"@0]     | ("Danny Green")       |
      | [:like "Dejounte Murray"->"LeBron James"@0]      | ("Marco Belinelli")   |
      | [:like "Dejounte Murray"->"Manu Ginobili"@0]     | ("LeBron James")      |
      | [:like "Dejounte Murray"->"Marco Belinelli"@0]   | ("Manu Ginobili")     |
      | [:like "Dejounte Murray"->"Russell Westbrook"@0] | ("Russell Westbrook") |
      | [:like "Dejounte Murray"->"Tony Parker"@0]       | ("Tony Parker")       |
    Then the result should be, in any order, with relax comparison:
      | nodes                 | relationships                               |
      | [("Tim Duncan")]      | [[:like "Dejounte Murray"->"Tim Duncan"@0]] |
      | [("Dejounte Murray")] | <[edge2]>                                   |
      | <[vertex3]>           | []                                          |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM 'Tim Duncan' IN like, serve
        WHERE like.likeness > 80
        YIELD vertices as nodes, edges as relationships
      """
    Then define some list variables:
      | edge1                                     | vertex2             | edge2                                        | vertex3            |
      | [:like "Dejounte Murray"->"Tim Duncan"@0] | ("Manu Ginobili")   | [:like "Dejounte Murray"->"Manu Ginobili"@0] | ("Tiago Splitter") |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]   | ("Dejounte Murray") | [:like "Tiago Splitter"->"Manu Ginobili"@0]  |                    |
      | [:like "Tony Parker"->"Tim Duncan"@0]     | ("Tony Parker")     | [:like "Tim Duncan"->"Manu Ginobili"@0]      |                    |
      |                                           |                     | [:like "Tony Parker"->"Manu Ginobili"@0]     |                    |
      |                                           |                     | [:like "Dejounte Murray"->"Tony Parker"@0]   |                    |
      |                                           |                     | [:like "Tim Duncan"->"Tony Parker"@0]        |                    |
    Then the result should be, in any order, with relax comparison:
      | nodes            | relationships |
      | [("Tim Duncan")] | <[edge1]>     |
      | <[vertex2]>      | <[edge2]>     |
      | <[vertex3]>      | []            |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM 'Tim Duncan', 'James Harden'  OUT serve
        WHERE serve.start_year > 2012
        YIELD vertices as nodes, edges as relationships
      """
    Then the result should be, in any order, with relax comparison:
      | nodes                                                                                                                                                                  | relationships |
      | [("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}), ("James Harden" :player{age: 29, name: "James Harden"})] | []            |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM 'Tim Duncan', 'James Harden'  OUT serve
        WHERE serve.start_year > 1996
        YIELD vertices as nodes, edges as relationships
      """
    Then the result should be, in any order, with relax comparison:
      | nodes                                                                                                                                                                  | relationships                                                                                                                                                                                                               |
      | [("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}), ("James Harden" :player{age: 29, name: "James Harden"})] | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "James Harden"->"Rockets" @0 {end_year: 2019, start_year: 2012}], [:serve "James Harden"->"Thunders" @0 {end_year: 2012, start_year: 2009}]] |
      | [("Spurs" :team{name: "Spurs"}), ("Rockets" :team{name: "Rockets"}), ("Thunders" :team{name: "Thunders"})]                                                             | []                                                                                                                                                                                                                          |

  Scenario: subgraph with tag filter
    When executing query:
      """
      GET SUBGRAPH WITH PROP FROM 'Tim Duncan' OUT like WHERE $$.player.age > 36 YIELD vertices as v, edges as e
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                                                                             | e                                                         |
      | [("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})] | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]] |
      | [("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})]                                                   | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]] |
    When executing query:
      """
      GO FROM 'Tim Duncan' over serve YIELD serve._src AS id
      | GET SUBGRAPH WITH PROP FROM $-.id
        WHERE $$.player.age > 36
        YIELD VERTICES as a, EDGES as b
      """
    Then the result should be, in any order, with relax comparison:
      | a                                                                                                                            | b                                                                                                                                                                                                                                                                                                                                                |
      | [("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"})]                | [[:teammate "Manu Ginobili"->"Tim Duncan" @0 {end_year: 2016, start_year: 2002}], [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}], [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}], [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]] |
      | [("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}), ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})] | []                                                                                                                                                                                                                                                                                                                                               |

  Scenario: subgraph with tag and edge filter
    When executing query:
      """
      GO FROM 'Tim Duncan' over serve YIELD serve._src AS id
      | GET SUBGRAPH WITH PROP FROM $-.id
        WHERE $$.player.age > 36 AND like.likeness > 80
        YIELD VERTICES as a, EDGES as b
      """
    Then the result should be, in any order, with relax comparison:
      | a                                                                                                             | b                                                                                                                  |
      | [("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})] | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]] |
      | [("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})]                                                   | []                                                                                                                 |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 5 steps from 'Tony Parker' BOTH like
        WHERE $$.player.age > 36 AND like.likeness > 80
        YIELD VERTICES as nodes, EDGES as relationships
      """
    Then the result should be, in any order, with relax comparison:
      | nodes                                                                                                                                                                    | relationships                                                                                                                                                            |
      | [("Tony Parker" :player{age: 36, name: "Tony Parker"})]                                                                                                                  | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}], [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]] |
      | [("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}), ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})] | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]                                                       |

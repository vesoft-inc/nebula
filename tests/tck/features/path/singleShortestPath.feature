# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Single Shortest Path

  Background:
    Given a graph with space named "nba"

  Scenario: Single Shortest Path zero step
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan" , "Yao Ming" TO "Tony Parker" OVER like UPTO 0 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER * UPTO 0 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan", "Yao Ming" TO "Tony Parker", "Spurs" OVER * UPTO 0 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan", "Yao Ming" TO "Tony Parker", "Spurs" OVER * UPTO -1 STEPS YIELD path as p
      """
    Then a SyntaxError should be raised at runtime: syntax error near `-1 STEPS'

  Scenario: [1] Single Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER * BIDIRECT YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                  |
      | <("Tim Duncan")<-[:like@0 {}]-("Tony Parker")>     |
      | <("Tim Duncan")<-[:teammate@0 {}]-("Tony Parker")> |
      | <("Tim Duncan")-[:like@0 {}]->("Tony Parker")>     |
      | <("Tim Duncan")-[:teammate@0 {}]->("Tony Parker")> |
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER * BIDIRECT YIELD path as p
      | YIELD startnode($-.p), endnode($-.p)
      """
    Then the result should be, in any order, with relax comparison:
      | startnode($-.p) | endnode($-.p)   |
      | ("Tim Duncan")  | ("Tony Parker") |

  Scenario: [2] Single Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan",  "Joel Embiid" TO "Giannis Antetokounmpo", "Yao Ming" OVER * BIDIRECT UPTO 18 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                       |
      | <("Tim Duncan")<-[:like@0 {}]-("Shaquille O'Neal")<-[:like@0 {}]-("Yao Ming")>                                                                                                                          |
      | <("Tim Duncan")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>                                                               |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:like@0 {}]->("Tim Duncan")<-[:like@0 {}]-("Shaquille O'Neal")<-[:like@0 {}]-("Yao Ming")>                                |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Tiago Splitter")-[:like@0 {}]->("Tim Duncan")<-[:like@0 {}]-("Shaquille O'Neal")<-[:like@0 {}]-("Yao Ming")>                                 |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Jonathon Simmons")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Tracy McGrady")<-[:like@0 {}]-("Yao Ming")>                                     |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Tracy McGrady")<-[:like@0 {}]-("Yao Ming")>                                      |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@1 {}]->("Spurs")<-[:serve@0 {}]-("Tracy McGrady")<-[:like@0 {}]-("Yao Ming")>                                      |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Tiago Splitter")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Tracy McGrady")<-[:like@0 {}]-("Yao Ming")>                                       |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Jonathon Simmons")-[:serve@0 {}]->("Magic")<-[:serve@0 {}]-("Tracy McGrady")<-[:like@0 {}]-("Yao Ming")>                                     |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Jonathon Simmons")-[:serve@0 {}]->("Magic")<-[:serve@0 {}]-("Shaquille O'Neal")<-[:like@0 {}]-("Yao Ming")>                                  |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@0 {}]->("Raptors")<-[:serve@0 {}]-("Tracy McGrady")<-[:like@0 {}]-("Yao Ming")>                                    |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@0 {}]->("Bulls")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>  |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Jonathon Simmons")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")> |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>  |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@1 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>  |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Tiago Splitter")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>   |
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan",  "Joel Embiid" TO "Giannis Antetokounmpo", "Yao Ming" OVER * BIDIRECT UPTO 18 STEPS YIELD path as p
      | YIELD startnode($-.p), endnode($-.p)
      """
    Then the result should be, in any order, with relax comparison:
      | startnode($-.p) | endnode($-.p)             |
      | ("Tim Duncan")  | ("Yao Ming")              |
      | ("Tim Duncan")  | ("Giannis Antetokounmpo") |
      | ("Joel Embiid") | ("Yao Ming")              |
      | ("Joel Embiid") | ("Giannis Antetokounmpo") |

  Scenario: single shortest Path With Limit
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Shaquille O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 2
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                         |
      | <("Shaquille O'Neal")-[:serve]->("Lakers")>                               |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan",  "Joel Embiid" TO "Giannis Antetokounmpo", "Yao Ming" OVER * BIDIRECT UPTO 18 STEPS YIELD path as p
      | ORDER by $-.p | LIMIT 2
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                      |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Jonathon Simmons")-[:serve@0 {}]->("Magic")<-[:serve@0 {}]-("Shaquille O'Neal")<-[:like@0 {}]-("Yao Ming")> |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Jonathon Simmons")-[:serve@0 {}]->("Magic")<-[:serve@0 {}]-("Tracy McGrady")<-[:like@0 {}]-("Yao Ming")>    |
    When executing query:
      """
      FIND SINGLE SHORTEST PATH FROM "Tim Duncan",  "Joel Embiid" TO "Giannis Antetokounmpo", "Yao Ming" OVER * BIDIRECT UPTO 18 STEPS YIELD path as p
      | LIMIT 3 | YIELD count(*) as num
      """
    Then the result should be, in any order, with relax comparison:
      | num |
      | 3   |
    When executing query:
      """
      $start = LOOKUP ON player WHERE player.age > 30 YIELD id(vertex) AS id;
      $end = LOOKUP ON player WHERE player.age <= 30 YIELD id(vertex) AS id;
      FIND SINGLE SHORTEST PATH FROM $start.id TO $end.id OVER * BIDIRECT YIELD path AS p | LIMIT 0 | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 0        |
    When executing query:
      """
      $start = LOOKUP ON player WHERE player.age > 30 YIELD id(vertex) AS id;
      $end = LOOKUP ON player WHERE player.age <= 30 YIELD id(vertex) AS id;
      FIND SINGLE SHORTEST PATH FROM $start.id TO $end.id OVER * BIDIRECT YIELD path AS p | LIMIT 174 | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 174      |
    When executing query:
      """
      $start = LOOKUP ON player WHERE player.age > 30 YIELD id(vertex) AS id;
      $end = LOOKUP ON player WHERE player.age <= 30 YIELD id(vertex) AS id;
      FIND SINGLE SHORTEST PATH FROM $start.id TO $end.id OVER * BIDIRECT YIELD path AS p | LIMIT 0, 174 | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 174      |
    When executing query:
      """
      $start = LOOKUP ON player WHERE player.age > 30 YIELD id(vertex) AS id;
      $end = LOOKUP ON player WHERE player.age <= 30 YIELD id(vertex) AS id;
      FIND SINGLE SHORTEST PATH FROM $start.id TO $end.id OVER * BIDIRECT YIELD path AS p | LIMIT 100, 174 | YIELD count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | count(*) |
      | 174      |

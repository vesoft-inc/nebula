# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: All Path

  Scenario: [1] ALL Path
    Given a graph with space named "nba"
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tim Duncan" OVER * UPTO 2 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                           |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>           |
      | <("Tim Duncan")-[:teammate]->("Tony Parker")-[:like]->("Tim Duncan")>       |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:teammate]->("Tim Duncan")>       |
      | <("Tim Duncan")-[:teammate]->("Tony Parker")-[:teammate]->("Tim Duncan")>   |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>         |
      | <("Tim Duncan")-[:teammate]->("Manu Ginobili")-[:like]->("Tim Duncan")>     |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:teammate]->("Tim Duncan")>     |
      | <("Tim Duncan")-[:teammate]->("Manu Ginobili")-[:teammate]->("Tim Duncan")> |
      | <("Tim Duncan")-[:teammate]->("Danny Green")-[:like]->("Tim Duncan")>       |
      | <("Tim Duncan")-[:teammate]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")> |
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker", "Manu Ginobili" OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")>                                                       |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")>                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>      |
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")>                          |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:serve]->("Spurs")>                                                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>                                   |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:serve]->("Spurs")>                                     |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>             |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>           |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:serve]->("Spurs")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>          |

  Scenario: [1] ALL Path Run Time Input
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst |
      FIND ALL PATH FROM $-.src TO $-.dst OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                                         |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>      |
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                                         |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>      |

  Scenario: [1] ALL Path With Limit
    Given a graph with space named "nba"
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                            |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>      |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 5
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |

  Scenario: [1] ALL PATH REVERSELY
    Given a graph with space named "nba"
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like REVERSELY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like REVERSELY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                         |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                          |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                              |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>      |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")> |
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like REVERSELY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                       |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                               |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>                                                         |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")>                                |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                                    |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")>     |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")> |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>            |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>      |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>        |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>       |

  Scenario: [2] ALL PATH BIDIRECT
    Given a graph with space named "nba"
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like BIDIRECT UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                   |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                           |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                           |
      | <("Tim Duncan")<-[:like]-("Marco Belinelli")-[:like]->("Tony Parker")>                              |
      | <("Tim Duncan")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")>                              |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                            |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")-[:like]->("Tony Parker")>                            |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("Boris Diaw")-[:like]->("Tony Parker")>                                   |
      | <("Tim Duncan")<-[:like]-("Danny Green")<-[:like]-("Marco Belinelli")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")<-[:like]-("Danny Green")-[:like]->("Marco Belinelli")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")<-[:like]-("Dejounte Murray")-[:like]->("Marco Belinelli")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")>   |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Danny Green")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")<-[:like]-("Marco Belinelli")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>   |
      | <("Tim Duncan")-[:like]->("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")>   |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>        |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")<-[:like]-("Tony Parker")>        |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")-[:like]->("Tony Parker")>        |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>        |
      | <("Tim Duncan")<-[:like]-("Dejounte Murray")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Tiago Splitter")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>    |

  Scenario: ALL Path WITH PROP
    Given a graph with space named "nba"
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                                                                       |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})-[:like@0 {likeness: 90}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                               |

  Scenario: ALL Path WITH FILTER
    Given a graph with space named "nba"
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Yao Ming" OVER * BIDIRECT
      WHERE (like.likeness >= 80 and like.likeness <= 90) OR (teammate.start_year is not EMPTY and teammate.start_year > 2001) UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tony Parker" TO "Yao Ming" OVER * BIDIRECT
      WHERE  teammate.start_year > 2000 OR (like.likeness is not EMPTY AND like.likeness >= 80) UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                                         |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 95}]-("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                         |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2001}]-("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                         |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Yao Ming" TO "Danny Green" OVER * BIDIRECT
      WHERE like.likeness is  EMPTY OR like.likeness >= 80 UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                                                                                         |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2010, start_year: 2009}]->("Cavaliers" :team{name: "Cavaliers"})<-[:serve@0 {end_year: 2010, start_year: 2009}]-("Danny Green" :player{age: 31, name: "Danny Green"})>                                                    |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2010}]->("Danny Green" :player{age: 31, name: "Danny Green"})> |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2000, start_year: 1997}]->("Raptors" :team{name: "Raptors"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Danny Green" :player{age: 31, name: "Danny Green"})>                                                              |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2018, start_year: 2010}]-("Danny Green" :player{age: 31, name: "Danny Green"})>                                                                  |

  Scenario: Dangling edge
    Given an empty graph
    And load "nba" csv data to a new space
    When executing query:
      """
      INSERT EDGE like(likeness) VALUES "Tim Duncan"->"Tim Parker":(99);
      INSERT EDGE like(likeness) VALUES "Tim Parker"->"Tony Parker":(90);
      """
    Then the execution should be successful
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 2 steps YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                      |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                          |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 99}]->("Tim Parker")-[:like@0 {likeness: 90}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like BIDIRECT UPTO 2 steps YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 95}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 99}]->("Tim Parker")-[:like@0 {likeness: 90}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                    |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 55}]-("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})-[:like@0 {likeness: 50}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 99}]-("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 75}]-("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})<-[:like@0 {likeness: 90}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 75}]-("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 90}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})<-[:like@0 {likeness: 95}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})>         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})<-[:like@0 {likeness: 95}]-("Tony Parker" :player{age: 36, name: "Tony Parker"})>         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:like@0 {likeness: 80}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>               |
    Then drop the used space

  Scenario: ALL PATH YIELD PATH
    Given a graph with space named "nba"
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Yao Ming" TO "Danny Green" OVER * BIDIRECT
      WHERE like.likeness is  EMPTY OR like.likeness >= 80 UPTO 3 STEPS YIELD path as p |
      YIELD startnode($-.p) as startnode
      """
    Then the result should be, in any order, with relax comparison:
      | startnode                                       |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Yao Ming" TO "Danny Green" OVER * BIDIRECT
      WHERE like.likeness is  EMPTY OR like.likeness >= 80 UPTO 3 STEPS YIELD path as p |
      YIELD endnode($-.p) as endnode
      """
    Then the result should be, in any order, with relax comparison:
      | endnode                                               |
      | ("Danny Green" :player{age: 31, name: "Danny Green"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"}) |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS YIELD path as p |
      YIELD length($-.p) as length
      """
    Then the result should be, in any order, with relax comparison:
      | length |
      | 1      |
      | 3      |
      | 3      |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS YIELD path as p |
      YIELD relationships($-.p) as relationships
      """
    Then the result should be, in any order, with relax comparison:
      | relationships                                                                                                                                                                       |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]                                                                                                                             |
      | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}], [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]           |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}], [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]] |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS YIELD path as p |
      YIELD nodes($-.p) as nodes
      """
    Then the result should be, in any order, with relax comparison:
      | nodes                                                                                                                                                                                                                                                              |
      | [("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"}), ("Tony Parker" :player{age: 36, name: "Tony Parker"})]                                                                                                                                    |
      | [("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"}), ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}), ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"}), ("Tony Parker" :player{age: 36, name: "Tony Parker"})] |
      | [("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"}), ("Tony Parker" :player{age: 36, name: "Tony Parker"}), ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}), ("Tony Parker" :player{age: 36, name: "Tony Parker"})]          |

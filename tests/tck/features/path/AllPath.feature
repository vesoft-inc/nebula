# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: All Path

  Background:
    Given a graph with space named "nba"

  Scenario: [1] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |

  Scenario: [2] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker", "Manu Ginobili" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")>                                                       |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")>                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>      |

  Scenario: [3] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")>                          |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |

  Scenario: [4] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
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
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND ALL PATH FROM $-.src TO $-.dst OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                                         |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>      |

  Scenario: [2] ALL Path Run Time Input
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                                         |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>      |

  Scenario: [1] ALL Path With Limit
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>      |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: [2] ALL Path With Limit
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 5
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |

  Scenario: [1] ALL PATH REVERSELY
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path |

  Scenario: [2] ALL PATH REVERSELY
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                         |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                          |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                              |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>      |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")> |

  Scenario: [3] ALL PATH REVERSELY
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                    |
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
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like BIDIRECT UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                |
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
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                                                                                                                                                                    |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})-[:like@0 {likeness: 90}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                               |

  Scenario: ALL Path WITH FILTER
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tim Duncan" TO "Yao Ming" OVER * BIDIRECT
      WHERE (like.likeness >= 80 and like.likeness <= 90) OR (teammate.start_year is not EMPTY and teammate.start_year > 2001) UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                            |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Tony Parker" TO "Yao Ming" OVER * BIDIRECT
      WHERE  teammate.start_year > 2000 OR (like.likeness is not EMPTY AND like.likeness >= 80) UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                                                                                                                                    |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 95}]-("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                         |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2001}]-("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                         |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
    When executing query:
      """
      FIND ALL PATH WITH PROP FROM "Yao Ming" TO "Danny Green" OVER * BIDIRECT
      WHERE like.likeness is  EMPTY OR like.likeness >= 80 UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                                                                                                                                    |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})-[:serve@0 {end_year: 2010, start_year: 2009}]->("Cavaliers" :team{name: "Cavaliers"})<-[:serve@0 {end_year: 2010, start_year: 2009}]-("Danny Green" :player{age: 31, name: "Danny Green"})>                                                    |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2010}]->("Danny Green" :player{age: 31, name: "Danny Green"})> |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2000, start_year: 1997}]->("Raptors" :team{name: "Raptors"})<-[:serve@0 {end_year: 2019, start_year: 2018}]-("Danny Green" :player{age: 31, name: "Danny Green"})>                                                            |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs" :team{name: "Spurs"})<-[:serve@0 {end_year: 2018, start_year: 2010}]-("Danny Green" :player{age: 31, name: "Danny Green"})>                                                                |

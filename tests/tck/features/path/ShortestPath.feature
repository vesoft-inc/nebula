# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Shortest Path

  Background:
    Given a graph with space named "nba"

  Scenario: [1] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                      |
      | <("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: [2] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "LaMarcus Aldridge" OVER like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [3] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                 |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [4] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER like, teammate
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                            |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:teammate]->("LaMarcus Aldridge")> |

  Scenario: [5] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER *
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                            |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:teammate]->("LaMarcus Aldridge")> |

  Scenario: [6] SinglePair Shortest Path limit steps
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "Tony Parker" OVER * UPTO 1 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "Tim Duncan" OVER * UPTO 1 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                         |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")> |

  Scenario: [1] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                      |
      | <("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:serve]->("Spurs")>      |

  Scenario: [2] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                          |
      | <("Tim Duncan")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")-[:teammate]->("Tony Parker")> |
      | <("Tim Duncan")-[:serve]->("Spurs")>          |

  Scenario: [3] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                      |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>                            |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")>                        |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                                             |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")>                                                          |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:teammate]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")>     |
      | <("Tony Parker")-[:teammate]->("Tim Duncan")-[:teammate]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                                                                              |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                                                                          |
      | <("Tony Parker")-[:serve]->("Spurs")>                                                                                     |

  Scenario: [4] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                               |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                      |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                                                       |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                                                   |
      | <("Tony Parker")-[:serve]->("Spurs")>                                                              |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Yao Ming" TO "Tim Duncan", "Spurs", "Lakers" OVER * UPTO 2 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")> |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>       |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")>    |

  Scenario: [5] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Marco Belinelli", "Yao Ming" TO "Spurs", "Lakers" OVER * UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                          |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                 |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")>                              |
      | <("Marco Belinelli")-[:like]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |
      | <("Marco Belinelli")-[:serve]->("Spurs")>                                                     |
      | <("Marco Belinelli")-[:serve@1]->("Spurs")>                                                   |

  Scenario: [6] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [7] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan", "Tiago Splitter" TO "Tony Parker","Spurs" OVER like,serve UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                  |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Tiago Splitter")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                             |
      | <("Tim Duncan")-[:serve]->("Spurs")>                                  |

  Scenario: [8] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Yao Ming"  TO "Tony Parker","Tracy McGrady" OVER like,serve UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                         |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Yao Ming")-[:like]->("Tracy McGrady")>                                                    |

  Scenario: [9] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                         |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>            |
      | <("Shaquile O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |

  Scenario: [10] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquile O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                         |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>            |
      | <("Shaquile O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |

  Scenario: [11] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER like UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")> |

  Scenario: [12] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Marco Belinelli" TO "Spurs", "Lakers" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                          |
      | <("Marco Belinelli")-[:serve]->("Spurs")>                                                     |
      | <("Marco Belinelli")-[:serve@1]->("Spurs")>                                                   |
      | <("Marco Belinelli")-[:like]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |

  Scenario: [1] MultiPair Shortest Path Empty Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like,serve UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path |

  Scenario: [1] MultiPair Shortest Path Run Time input
    When executing query:
      """
      YIELD "Yao Ming" AS src, "Tony Parker" AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like, serve UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                         |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: [2] MultiPair Shortest Path Run Time input
    When executing query:
      """
      YIELD "Shaquile O\'Neal" AS src
      | FIND SHORTEST PATH FROM $-.src TO "Manu Ginobili" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                         |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |

  Scenario: [3] MultiPair Shortest Path Run Time input
    When executing query:
      """
      YIELD "Manu Ginobili" AS dst
      | FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO $-.dst OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                         |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |

  Scenario: [4] MultiPair Shortest Path Run Time input
    When executing query:
      """
      GO FROM "Yao Ming" over like YIELD like._dst AS src
      | FIND SHORTEST PATH FROM $-.src TO "Tony Parker" OVER like, serve UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tracy McGrady")-[:like]->("Rudy Gay")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>                            |

  Scenario: [5] MultiPair Shortest Path Run Time input
    When executing query:
      """
      $a = GO FROM "Yao Ming" over like YIELD like._dst AS src;
      FIND SHORTEST PATH FROM $a.src TO "Tony Parker" OVER like, serve UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tracy McGrady")-[:like]->("Rudy Gay")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>                            |

  Scenario: [6] MultiPair Shortest Path Run Time input
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |

  Scenario: [7] MultiPair Shortest Path Run Time input
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND SHORTEST PATH FROM $a.src TO $a.dst OVER like UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |

  Scenario: [8] MultiPair Shortest Path Run Time input
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over like YIELD like._src AS src;
      GO FROM "Tony Parker" OVER like YIELD like._src AS src, like._dst AS dst
      | FIND SHORTEST PATH FROM $a.src TO $-.dst OVER like UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")>                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [1] Shortest Path With Limit
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like,serve UPTO 3 STEPS | ORDER BY $-.path | LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | path |

  Scenario: [2] Shortest Path With Limit
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquile O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS
      | ORDER BY $-.path | LIMIT 2
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Shaquile O'Neal")-[:serve]->("Lakers")>                               |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")> |

  Scenario: [3] Shortest Path With Limit
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS
      | ORDER BY $-.path | LIMIT 1
      """
    Then the result should be, in any order, with relax comparison:
      | path                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |

  Scenario: [4] Shortest Path With Limit
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS
      | ORDER BY $-.path | LIMIT 10
      """
    Then the result should be, in any order, with relax comparison:
      | path                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |

  Scenario: [1] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path |

  Scenario: [2] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER like REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | path                                      |
      | <("Tim Duncan")<-[:like]-("Tony Parker")> |

  Scenario: [3] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "LaMarcus Aldridge" OVER like REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | path                                            |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")> |

  Scenario: [4] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                      |
      | <("Tim Duncan")<-[:like]-("Tony Parker")> |

  Scenario: [5] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | path                                             |
      | <("Tony Parker")<-[:teammate]-("Manu Ginobili")> |

  Scenario: [1] Shortest Path BIDIRECT
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like BIDIRECT UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path |

  Scenario: [2] Shortest Path BIDIRECT
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT UPTO 2 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                             |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>    |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")> |
      | <("Tony Parker")-[:serve]->("Spurs")>                            |
      | <("Tony Parker")<-[:teammate]-("Manu Ginobili")>                 |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                     |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                 |

  Scenario: [3] Shortest Path BIDIRECT
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                               |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")<-[:serve]-("Manu Ginobili")>          |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")<-[:like]-("Manu Ginobili")>     |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")<-[:teammate]-("Manu Ginobili")> |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                      |
      | <("Tony Parker")<-[:like]-("Tim Duncan")<-[:like]-("Shaquile O'Neal")-[:serve]->("Lakers")>        |
      | <("Tony Parker")<-[:teammate]-("Tim Duncan")<-[:like]-("Shaquile O'Neal")-[:serve]->("Lakers")>    |
      | <("Tony Parker")-[:like]->("Tim Duncan")<-[:like]-("Shaquile O'Neal")-[:serve]->("Lakers")>        |
      | <("Tony Parker")-[:teammate]->("Tim Duncan")<-[:like]-("Shaquile O'Neal")-[:serve]->("Lakers")>    |
      | <("Tony Parker")<-[:like]-("Dejounte Murray")-[:like]->("LeBron James")-[:serve]->("Lakers")>      |
      | <("Tony Parker")-[:serve]->("Spurs")<-[:serve]-("Paul Gasol")-[:serve]->("Lakers")>                |
      | <("Tony Parker")-[:serve]->("Hornets")<-[:serve]-("Dwight Howard")-[:serve]->("Lakers")>           |
      | <("Tony Parker")-[:serve]->("Spurs")>                                                              |
      | <("Tony Parker")<-[:teammate]-("Manu Ginobili")>                                                   |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                                                       |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                                                   |

  Scenario: Shortest Path With PROP
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tim Duncan" TO "LaMarcus Aldridge" OVER like
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                                      |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * REVERSELY
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2002}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT UPTO 2 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                       |
      | <("Yao Ming" : player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquile O'Neal" :player {age: 47,name: "Shaquile O'Neal"})-[:serve@0 {end_year: 2004, start_year: 1996}]->("Lakers": team{name: "Lakers"})> |
      | <("Yao Ming" : player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady": player{age: 39,name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs": team{name: "Spurs"})>        |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:serve@0 {end_year: 2018, start_year: 1999}]->("Spurs" :team{name: "Spurs"})>                                                                                      |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2002}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                        |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2018, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                        |

  Scenario: Shortest Path With Filter
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT WHERE like.likeness == 90 OR like.likeness is empty UPTO 2 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                       |
      | <("Yao Ming" : player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquile O'Neal" :player {age: 47,name: "Shaquile O'Neal"})-[:serve@0 {end_year: 2004, start_year: 1996}]->("Lakers": team{name: "Lakers"})> |
      | <("Yao Ming" : player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady": player{age: 39,name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs": team{name: "Spurs"})>        |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:serve@0 {end_year: 2018, start_year: 1999}]->("Spurs" :team{name: "Spurs"})>                                                                                      |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2002}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                        |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2018, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                        |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * REVERSELY WHERE like.likeness > 70
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                              |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 95}]-("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 90}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
    When executing query:
      """
      $a = GO FROM "Yao Ming" over like YIELD like._dst AS src;
      FIND SHORTEST PATH WITH PROP FROM $a.src TO "Tony Parker" OVER like, serve WHERE serve.start_year is EMPTY UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                                                              |
      | <("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:like@0 {likeness: 90}]->("Rudy Gay" :player{age: 32, name: "Rudy Gay"})-[:like@0 {likeness: 70}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                             |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT WHERE teammate.start_year is not EMPTY OR like.likeness > 90 UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2002}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                         |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2018, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |

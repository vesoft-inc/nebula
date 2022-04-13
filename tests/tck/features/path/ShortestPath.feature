# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Shortest Path

  Background:
    Given a graph with space named "nba"

  Scenario: [1] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER like YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                         |
      | <("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: [2] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "LaMarcus Aldridge" OVER like YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                        |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [3] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER like YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                    |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [4] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER like, teammate YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                               |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:teammate]->("LaMarcus Aldridge")> |

  Scenario: [5] SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER * YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                               |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:teammate]->("LaMarcus Aldridge")> |

  Scenario: [6] SinglePair Shortest Path limit steps
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "Tony Parker" OVER * UPTO 1 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "Tim Duncan" OVER * UPTO 1 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                            |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")> |

  Scenario: [1] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                         |
      | <("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:serve]->("Spurs")>      |

  Scenario: [2] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER * UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                             |
      | <("Tim Duncan")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")-[:teammate]->("Tony Parker")> |
      | <("Tim Duncan")-[:serve]->("Spurs")>          |

  Scenario: [3] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                         |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>                           |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")>                       |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                                             |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:serve]->("Lakers")>                                                         |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:teammate]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")>     |
      | <("Tony Parker")-[:teammate]->("Tim Duncan")-[:teammate]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                                                                              |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                                                                          |
      | <("Tony Parker")-[:serve]->("Spurs")>                                                                                     |

  Scenario: [4] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                   |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                       |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                                                        |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                                                    |
      | <("Tony Parker")-[:serve]->("Spurs")>                                                               |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Yao Ming" TO "Tim Duncan", "Spurs", "Lakers" OVER * UPTO 2 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                    |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")> |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>        |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:serve]->("Lakers")>    |

  Scenario: [5] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Marco Belinelli", "Yao Ming" TO "Spurs", "Lakers" OVER * UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                             |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                 |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:serve]->("Lakers")>                             |
      | <("Marco Belinelli")-[:like]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |
      | <("Marco Belinelli")-[:serve]->("Spurs")>                                                     |
      | <("Marco Belinelli")-[:serve@1]->("Spurs")>                                                   |

  Scenario: [6] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                        |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [7] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan", "Tiago Splitter" TO "Tony Parker","Spurs" OVER like,serve UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                     |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Tiago Splitter")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                             |
      | <("Tim Duncan")-[:serve]->("Spurs")>                                  |

  Scenario: [8] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Yao Ming"  TO "Tony Parker","Tracy McGrady" OVER like,serve UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                             |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Yao Ming")-[:like]->("Tracy McGrady")>                                                     |

  Scenario: [9] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquille O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                             |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>            |
      | <("Shaquille O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |

  Scenario: [10] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquille O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                             |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>            |
      | <("Shaquille O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |

  Scenario: [11] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquille O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER like UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                         |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")> |

  Scenario: [12] MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Marco Belinelli" TO "Spurs", "Lakers" OVER * UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                             |
      | <("Marco Belinelli")-[:serve]->("Spurs")>                                                     |
      | <("Marco Belinelli")-[:serve@1]->("Spurs")>                                                   |
      | <("Marco Belinelli")-[:like]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |

  Scenario: [1] MultiPair Shortest Path Empty Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like,serve UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |

  Scenario: [1] MultiPair Shortest Path Run Time input
    When executing query:
      """
      YIELD "Yao Ming" AS src, "Tony Parker" AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like, serve UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                             |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: [2] MultiPair Shortest Path Run Time input
    When executing query:
      """
      YIELD "Shaquille O\'Neal" AS src
      | FIND SHORTEST PATH FROM $-.src TO "Manu Ginobili" OVER * UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                             |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |

  Scenario: [3] MultiPair Shortest Path Run Time input
    When executing query:
      """
      YIELD "Manu Ginobili" AS dst
      | FIND SHORTEST PATH FROM "Shaquille O\'Neal" TO $-.dst OVER * UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                             |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |

  Scenario: [4] MultiPair Shortest Path Run Time input
    When executing query:
      """
      GO FROM "Yao Ming" over like YIELD like._dst AS src
      | FIND SHORTEST PATH FROM $-.src TO "Tony Parker" OVER like, serve UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Tracy McGrady")-[:like]->("Rudy Gay")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>                           |

  Scenario: [5] MultiPair Shortest Path Run Time input
    When executing query:
      """
      $a = GO FROM "Yao Ming" over like YIELD like._dst AS src;
      FIND SHORTEST PATH FROM $a.src TO "Tony Parker" OVER like, serve UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                 |
      | <("Tracy McGrady")-[:like]->("Rudy Gay")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>                           |

  Scenario: [6] MultiPair Shortest Path Run Time input
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                           |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |

  Scenario: [7] MultiPair Shortest Path Run Time input
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND SHORTEST PATH FROM $a.src TO $a.dst OVER like UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                           |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |

  Scenario: [8] MultiPair Shortest Path Run Time input
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over like YIELD like._src AS src;
      GO FROM "Tony Parker" OVER like YIELD like._src AS src, like._dst AS dst
      | FIND SHORTEST PATH FROM $a.src TO $-.dst OVER like UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                        |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")>                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [1] Shortest Path With Limit
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like,serve UPTO 3 STEPS YIELD path as p | ORDER BY $-.p | LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | p |

  Scenario: [2] Shortest Path With Limit
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquille O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 2
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                         |
      | <("Shaquille O'Neal")-[:serve]->("Lakers")>                               |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")> |

  Scenario: [3] Shortest Path With Limit
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst |
      FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 1
      """
    Then the result should be, in any order, with relax comparison:
      | p                                           |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |

  Scenario: [4] Shortest Path With Limit
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst |
      FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 10
      """
    Then the result should be, in any order, with relax comparison:
      | p                                           |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |

  Scenario: [1] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like REVERSELY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |

  Scenario: [2] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER like REVERSELY YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                         |
      | <("Tim Duncan")<-[:like]-("Tony Parker")> |

  Scenario: [3] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "LaMarcus Aldridge" OVER like REVERSELY YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                               |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")> |

  Scenario: [4] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve REVERSELY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                         |
      | <("Tim Duncan")<-[:like]-("Tony Parker")> |

  Scenario: [5] Shortest Path REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * REVERSELY YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                |
      | <("Tony Parker")<-[:teammate]-("Manu Ginobili")> |

  Scenario: [1] Shortest Path BIDIRECT
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like BIDIRECT UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order:
      | p |

  Scenario: [2] Shortest Path BIDIRECT
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT UPTO 2 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                 |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>     |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:serve]->("Lakers")> |
      | <("Tony Parker")-[:serve]->("Spurs")>                             |
      | <("Tony Parker")<-[:teammate]-("Manu Ginobili")>                  |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                      |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                  |

  Scenario: [3] Shortest Path BIDIRECT
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                   |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")<-[:serve]-("Manu Ginobili")>           |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")<-[:like]-("Manu Ginobili")>     |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:like]->("Tim Duncan")<-[:teammate]-("Manu Ginobili")> |
      | <("Yao Ming")-[:like]->("Shaquille O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                       |
      | <("Tony Parker")<-[:like]-("Tim Duncan")<-[:like]-("Shaquille O'Neal")-[:serve]->("Lakers")>        |
      | <("Tony Parker")<-[:teammate]-("Tim Duncan")<-[:like]-("Shaquille O'Neal")-[:serve]->("Lakers")>    |
      | <("Tony Parker")-[:like]->("Tim Duncan")<-[:like]-("Shaquille O'Neal")-[:serve]->("Lakers")>        |
      | <("Tony Parker")-[:teammate]->("Tim Duncan")<-[:like]-("Shaquille O'Neal")-[:serve]->("Lakers")>    |
      | <("Tony Parker")<-[:like]-("Dejounte Murray")-[:like]->("LeBron James")-[:serve]->("Lakers")>       |
      | <("Tony Parker")-[:serve]->("Spurs")<-[:serve]-("Paul Gasol")-[:serve]->("Lakers")>                 |
      | <("Tony Parker")-[:serve]->("Hornets")<-[:serve]-("Dwight Howard")-[:serve]->("Lakers")>            |
      | <("Tony Parker")-[:serve]->("Spurs")>                                                               |
      | <("Tony Parker")<-[:teammate]-("Manu Ginobili")>                                                    |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                                                        |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                                                    |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Joel Embiid"  TO "Giannis Antetokounmpo" OVER * BIDIRECT UPTO 18 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                       |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@0 {}]->("Bulls")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>  |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Jonathon Simmons")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")> |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>  |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Marco Belinelli")-[:serve@1 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>  |
      | <("Joel Embiid")-[:serve@0 {}]->("76ers")<-[:serve@0 {}]-("Tiago Splitter")-[:serve@0 {}]->("Spurs")<-[:serve@0 {}]-("Paul Gasol")-[:serve@0 {}]->("Bucks")<-[:serve@0 {}]-("Giannis Antetokounmpo")>   |
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

  Scenario: Shortest Path With PROP
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tim Duncan" TO "LaMarcus Aldridge" OVER like YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * REVERSELY  YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                   |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2002}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT UPTO 2 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                            |
      | <("Yao Ming" : player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player {age: 47,name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2004, start_year: 1996}]->("Lakers": team{name: "Lakers"})> |
      | <("Yao Ming" : player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady": player{age: 39,name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs": team{name: "Spurs"})>          |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:serve@0 {end_year: 2018, start_year: 1999}]->("Spurs" :team{name: "Spurs"})>                                                                                        |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2002}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                          |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                  |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2018, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                          |

  Scenario: Shortest Path With Filter
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT WHERE like.likeness == 90 OR like.likeness is empty UPTO 2 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                            |
      | <("Yao Ming" : player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player {age: 47,name: "Shaquille O'Neal"})-[:serve@0 {end_year: 2004, start_year: 1996}]->("Lakers": team{name: "Lakers"})> |
      | <("Yao Ming" : player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady": player{age: 39,name: "Tracy McGrady"})-[:serve@0 {end_year: 2013, start_year: 2013}]->("Spurs": team{name: "Spurs"})>          |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:serve@0 {end_year: 2018, start_year: 1999}]->("Spurs" :team{name: "Spurs"})>                                                                                        |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2002}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                          |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2018, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                          |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * REVERSELY WHERE like.likeness > 70 YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                 |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:like@0 {likeness: 95}]-("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 90}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
    When executing query:
      """
      $a = GO FROM "Yao Ming" over like YIELD like._dst AS src;
      FIND SHORTEST PATH WITH PROP FROM $a.src TO "Tony Parker" OVER like, serve WHERE serve.start_year is EMPTY UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                                                 |
      | <("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:like@0 {likeness: 90}]->("Rudy Gay" :player{age: 32, name: "Rudy Gay"})-[:like@0 {likeness: 70}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})-[:like@0 {likeness: 75}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                           |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT WHERE teammate.start_year is not EMPTY OR like.likeness > 90 UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                   |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})<-[:teammate@0 {end_year: 2016, start_year: 2002}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                         |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2018, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |

  Scenario: Shortest Path YIELD path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquille O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                             |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>            |
      | <("Shaquille O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquille O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquille O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS YIELD path as p |
      YIELD length($-.p) as length
      """
    Then the result should be, in any order, with relax comparison:
      | length |
      | 2      |
      | 1      |
      | 2      |
      | 2      |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT WHERE teammate.start_year is not EMPTY OR like.likeness > 90 UPTO 3 STEPS YIELD path as p |
      YIELD nodes($-.p) as nodes
      """
    Then the result should be, in any order, with relax comparison:
      | nodes                                                                                                              |
      | [("Tony Parker" :player{age: 36, name: "Tony Parker"}), ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})] |
      | [("Tony Parker" :player{age: 36, name: "Tony Parker"}), ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})] |
      | [("Tony Parker" :player{age: 36, name: "Tony Parker"}), ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})] |
    When executing query:
      """
      FIND SHORTEST PATH WITH PROP FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * BIDIRECT WHERE teammate.start_year is not EMPTY OR like.likeness > 90 UPTO 3 STEPS YIELD path as p |
      YIELD distinct nodes($-.p) as nodes
      """
    Then the result should be, in any order, with relax comparison:
      | nodes                                                                                                              |
      | [("Tony Parker" :player{age: 36, name: "Tony Parker"}), ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})] |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "Tim Duncan" OVER * UPTO 1 STEPS YIELD path as p |
      YIELD relationships($-.p) as relationships
      """
    Then the result should be, in any order, with relax comparison:
      | relationships                                  |
      | [[:like "Tiago Splitter"->"Tim Duncan" @0 {}]] |

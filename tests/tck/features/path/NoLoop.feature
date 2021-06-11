# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: NoLoop Path

  Background:
    Given a graph with space named "nba"

  Scenario: [1] NOLOOP Path
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                      |
      | <("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: [2] NOLOOP Path
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Tony Parker", "Manu Ginobili" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                 |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                            |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")>                          |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")> |

  Scenario: [3] NOLOOP Path
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: [4] NOLOOP Path
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                         |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                    |
      | <("Tim Duncan")-[:serve]->("Spurs")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:serve]->("Spurs")>                                |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:serve]->("Spurs")> |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>     |

  Scenario: [1] NOLOOP Path Run Time Input
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND NOLOOP PATH FROM $-.src TO $-.dst OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>     |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")> |

  Scenario: [2] NOLOOP Path Run Time Input
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND NOLOOP PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>     |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")> |

  Scenario: [1] NOLOOP Path With Limit
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                            |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")> |
      | <("Tim Duncan")-[:serve]->("Spurs")>                            |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                       |

  Scenario: [2] NOLOOP Path With Limit
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND NOLOOP PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 5
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>     |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                |

  Scenario: [1] NOLOOP Path REVERSELY
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path |

  Scenario: [2] NOLOOP Path REVERSELY
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Tony Parker" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                     |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")> |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>     |

  Scenario: [3] NOLOOP Path REVERSELY
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                           |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>                                                     |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")>                            |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                            |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")> |

  Scenario: [2] NOLOOP Path BIDIRECT
    When executing query:
      """
      FIND NOLOOP PATH FROM "Tim Duncan" TO "Tony Parker" OVER like BIDIRECT UPTO 3 STEPS
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
      | <("Tim Duncan")<-[:like]-("Dejounte Murray")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Tiago Splitter")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>    |

  Scenario: NOLOOP Path WITH PROP
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                          |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                                      |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |

  Scenario: NOLOOP Path WITH FILTER
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker" OVER like BIDIRECT WHERE like.likeness > 95 UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                                                                                                                  |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 99}]-("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM "Tim Duncan" TO "Tony Parker", "Spurs" OVER like, serve WHERE serve.start_year > 1990 OR like.likeness is EMPTY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                        |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})> |
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND NOLOOP PATH WITH PROP FROM $a.src TO $a.dst OVER like WHERE like.likeness > 90 UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 5
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                                                                                                          |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |

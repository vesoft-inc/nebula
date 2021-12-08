# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Integer Vid NoLoop Path

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Integer Vid [1] NOLOOP Path
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                         |
      | <("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: Integer Vid [2] NOLOOP Path
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("Manu Ginobili") OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                    |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                            |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")>                          |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")> |

  Scenario: Integer Vid [3] NOLOOP Path
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("LaMarcus Aldridge") OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                        |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: Integer Vid [4] NOLOOP Path
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("Spurs") OVER like,serve UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                            |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                    |
      | <("Tim Duncan")-[:serve]->("Spurs")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:serve]->("Spurs")>                                |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:serve]->("Spurs")> |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>     |

  Scenario: Integer Vid [1] NOLOOP Path Run Time Input
    When executing query:
      """
      GO FROM hash("Tim Duncan") over * YIELD like._dst AS src, serve._src AS dst
      | FIND NOLOOP PATH FROM $-.src TO $-.dst OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>     |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")> |

  Scenario: Integer Vid [2] NOLOOP Path Run Time Input
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") over * YIELD like._dst AS src, serve._src AS dst;
      FIND NOLOOP PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>     |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")> |

  Scenario: Integer Vid [1] NOLOOP Path With Limit
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("Spurs") OVER like,serve UPTO 3 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                            |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                    |
      | < ("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>    |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:serve]->("Spurs")> |

  Scenario: Integer Vid [2] NOLOOP Path With Limit
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") over * YIELD like._dst AS src, serve._src AS dst;
      FIND NOLOOP PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 5
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>     |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                |

  Scenario: Integer Vid [1] NOLOOP Path REVERSELY
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Nobody"), hash("Spur") OVER like REVERSELY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p |

  Scenario: Integer Vid [2] NOLOOP Path REVERSELY
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like REVERSELY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                        |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")> |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>     |

  Scenario: Integer Vid [3] NOLOOP Path REVERSELY
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("LaMarcus Aldridge") OVER like REVERSELY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                   |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                           |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>                                                     |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")>                            |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                            |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")> |

  Scenario: Integer Vid [2] NOLOOP Path BIDIRECT
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like BIDIRECT UPTO 3 STEPS YIELD path as p
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
      | <("Tim Duncan")<-[:like]-("Dejounte Murray")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Tiago Splitter")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>    |

  Scenario: Integer Vid NOLOOP Path WITH PROP
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("LaMarcus Aldridge") OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |

  Scenario: Integer Vid NOLOOP Path WITH FILTER
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like BIDIRECT WHERE like.likeness > 95 UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                                                                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 99}]-("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("Spurs") OVER like, serve WHERE serve.start_year > 1990 OR like.likeness is EMPTY UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                           |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})> |
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") over * YIELD like._dst AS src, serve._src AS dst;
      FIND NOLOOP PATH WITH PROP FROM $a.src TO $a.dst OVER like WHERE like.likeness > 90 UPTO 3 STEPS YIELD path as p |
      ORDER BY $-.p | LIMIT 5
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                             |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |

  Scenario: Integer Vid NOLOOP Path YIELD PATH
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like UPTO 3 STEPS YIELD path as p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})> |
    When executing query:
      """
      FIND NOLOOP PATH WITH PROP FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("LaMarcus Aldridge") OVER like UPTO 3 STEPS YIELD path as p |
      YIELD startnode($-.p) as startnode
      """
    Then the result should be, in any order, with relax comparison:
      | startnode                                                                                                   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") over * YIELD like._dst AS src, serve._src AS dst;
      FIND NOLOOP PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS YIELD path as p |
      YIELD nodes($-.p) as nodes
      """
    Then the result should be, in any order, with relax comparison:
      | nodes                                                    |
      | [("Manu Ginobili"), ("Tim Duncan")]                      |
      | [("Tony Parker"), ("Tim Duncan")]                        |
      | [("Tony Parker"), ("Manu Ginobili"), ("Tim Duncan")]     |
      | [("Tony Parker"), ("LaMarcus Aldridge"), ("Tim Duncan")] |
    When executing query:
      """
      FIND NOLOOP PATH FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like REVERSELY UPTO 3 STEPS YIELD path as p |
      YIELD relationships($-.p) as relationships
      """
    Then the result should be, in any order, with relax comparison:
      | relationships                                                                                       |
      | [[:like "Tony Parker"->"Tim Duncan" @0 {}]]                                                         |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan" @0 {}], [:like "Tony Parker"->"LaMarcus Aldridge" @0 {}]] |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {}], [:like "Tony Parker"->"Manu Ginobili" @0 {}]]         |

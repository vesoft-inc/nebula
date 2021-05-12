# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Variable length Pattern match (m to n)

  Background:
    Given a graph with space named "nba"

  Scenario: single both direction edge with properties
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3{start_year: 2000}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e | v |
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:like*2..3{likeness: 90}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                  | v                  |
      | [[:like "Tim Duncan"<-"Manu Ginobili"], [:like "Manu Ginobili"<-"Tiago Splitter"]] | ("Tiago Splitter") |

  Scenario: single direction edge with properties
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3{start_year: 2000}]->(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e | v |
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:like*2..3{likeness: 90}]->(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e | v |
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})<-[e:like*2..3{likeness: 90}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                  | v                  |
      | [[:like "Tim Duncan"<-"Manu Ginobili"], [:like "Manu Ginobili"<-"Tiago Splitter"]] | ("Tiago Splitter") |

  Scenario: single both direction edge without properties
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                      | v                     |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Dejounte Murray"]]                                                  | ("Dejounte Murray")   |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"]]                                                  | ("Marco Belinelli")   |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"Bulls"]]             | ("Bulls")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"Hornets"]]           | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"Hawks"]]             | ("Hawks")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"76ers"]]             | ("76ers")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"Spurs"@1]]           | ("Spurs")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"Hornets"@1]]         | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"Raptors"]]           | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"Warriors"]]          | ("Warriors")          |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"], [:serve "Marco Belinelli"->"Kings"]]             | ("Kings")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Danny Green"]]                                                      | ("Danny Green")       |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Danny Green"], [:serve "Danny Green"->"Cavaliers"]]                 | ("Cavaliers")         |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Danny Green"], [:serve "Danny Green"->"Raptors"]]                   | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Aron Baynes"]]                                                      | ("Aron Baynes")       |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Aron Baynes"], [:serve "Aron Baynes"->"Pistons"]]                   | ("Pistons")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Aron Baynes"], [:serve "Aron Baynes"->"Celtics"]]                   | ("Celtics")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Jonathon Simmons"]]                                                 | ("Jonathon Simmons")  |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Jonathon Simmons"], [:serve "Jonathon Simmons"->"76ers"]]           | ("76ers")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Jonathon Simmons"], [:serve "Jonathon Simmons"->"Magic"]]           | ("Magic")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Rudy Gay"]]                                                         | ("Rudy Gay")          |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Rudy Gay"], [:serve "Rudy Gay"->"Raptors"]]                         | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Rudy Gay"], [:serve "Rudy Gay"->"Kings"]]                           | ("Kings")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Rudy Gay"], [:serve "Rudy Gay"->"Grizzlies"]]                       | ("Grizzlies")         |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tony Parker"]]                                                      | ("Tony Parker")       |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tony Parker"], [:serve "Tony Parker"->"Hornets"]]                   | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Manu Ginobili"]]                                                    | ("Manu Ginobili")     |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"David West"]]                                                       | ("David West")        |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"David West"], [:serve "David West"->"Pacers"]]                      | ("Pacers")            |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"David West"], [:serve "David West"->"Warriors"]]                    | ("Warriors")          |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"David West"], [:serve "David West"->"Hornets"]]                     | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tracy McGrady"]]                                                    | ("Tracy McGrady")     |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tracy McGrady"], [:serve "Tracy McGrady"->"Raptors"]]               | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tracy McGrady"], [:serve "Tracy McGrady"->"Magic"]]                 | ("Magic")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tracy McGrady"], [:serve "Tracy McGrady"->"Rockets"]]               | ("Rockets")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1]]                                                | ("Marco Belinelli")   |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"Bulls"]]           | ("Bulls")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"Spurs"]]           | ("Spurs")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"Hornets"]]         | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"Hawks"]]           | ("Hawks")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"76ers"]]           | ("76ers")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"Hornets"]]         | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"Raptors"]]         | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"Warriors"]]        | ("Warriors")          |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Marco Belinelli"@1], [:serve "Marco Belinelli"->"Kings"]]           | ("Kings")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Paul Gasol"]]                                                       | ("Paul Gasol")        |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Paul Gasol"], [:serve "Paul Gasol"->"Lakers"]]                      | ("Lakers")            |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Paul Gasol"], [:serve "Paul Gasol"->"Bulls"]]                       | ("Bulls")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Paul Gasol"], [:serve "Paul Gasol"->"Grizzlies"]]                   | ("Grizzlies")         |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Paul Gasol"], [:serve "Paul Gasol"->"Bucks"]]                       | ("Bucks")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"LaMarcus Aldridge"]]                                                | ("LaMarcus Aldridge") |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"LaMarcus Aldridge"], [:serve "LaMarcus Aldridge"->"Trail Blazers"]] | ("Trail Blazers")     |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tiago Splitter"]]                                                   | ("Tiago Splitter")    |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tiago Splitter"], [:serve "Tiago Splitter"->"Hawks"]]               | ("Hawks")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Tiago Splitter"], [:serve "Tiago Splitter"->"76ers"]]               | ("76ers")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Cory Joseph"]]                                                      | ("Cory Joseph")       |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Cory Joseph"], [:serve "Cory Joseph"->"Pacers"]]                    | ("Pacers")            |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Cory Joseph"], [:serve "Cory Joseph"->"Raptors"]]                   | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Kyle Anderson"]]                                                    | ("Kyle Anderson")     |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Kyle Anderson"], [:serve "Kyle Anderson"->"Grizzlies"]]             | ("Grizzlies")         |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Boris Diaw"]]                                                       | ("Boris Diaw")        |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Boris Diaw"], [:serve "Boris Diaw"->"Suns"]]                        | ("Suns")              |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Boris Diaw"], [:serve "Boris Diaw"->"Jazz"]]                        | ("Jazz")              |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Boris Diaw"], [:serve "Boris Diaw"->"Hawks"]]                       | ("Hawks")             |
      | [[:serve "Tim Duncan"->"Spurs"], [:serve "Spurs"<-"Boris Diaw"], [:serve "Boris Diaw"->"Hornets"]]                     | ("Hornets")           |

  Scenario: single both direction edge without properties
    When executing query:
      """
      MATCH (:player{name: "Tim Duncan"})-[e:like*2..3]-(v)
      RETURN e |
      YIELD COUNT(*)
      """
    Then the result should be, in any order:
      | COUNT(*) |
      | 292      |

  Scenario: single direction edge without properties
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3]->(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e | v |
    When executing query:
      """
      MATCH (:player{name: "Tim Duncan"})-[e:like*2..3]->(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                             | v                     |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Tim Duncan"]]                                                    | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Tim Duncan"], [:like "Tim Duncan"->"Manu Ginobili"]]             | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Manu Ginobili"]]                                                 | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"]]          | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"]]                                             | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"], [:like "LaMarcus Aldridge"->"Tony Parker"]] | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"], [:like "LaMarcus Aldridge"->"Tim Duncan"]]  | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"]]                                                | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"], [:like "Tim Duncan"->"Tony Parker"]]           | ("Tony Parker")       |

  Scenario: multiple both direction edge with properties
    When executing query:
      """
      MATCH (:player{name: "Tim Duncan"})-[e:serve|like*2..3{likeness: 90}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                  | v                  |
      | [[:like "Tim Duncan"<-"Manu Ginobili"], [:like "Manu Ginobili"<-"Tiago Splitter"]] | ("Tiago Splitter") |
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3{start_year: 2000}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e | v |

  Scenario: multiple direction edge with properties
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3{likeness: 90}]->(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e | v |
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})<-[e:serve|like*2..3{likeness: 90}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                  | v                  |
      | [[:like "Tim Duncan"<-"Manu Ginobili"], [:like "Manu Ginobili"<-"Tiago Splitter"]] | ("Tiago Splitter") |

  Scenario: multiple both direction edge without properties
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3]-(v)
      RETURN e |
      YIELD COUNT(*)
      """
    Then the result should be, in any order:
      | COUNT(*) |
      | 927      |

  Scenario: multiple direction edge without properties
    When executing query:
      """
      MATCH (:player{name: "Tim Duncan"})-[e:serve|like*2..3]->(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                                | v                     |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Tim Duncan"]]                                                       | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Tim Duncan"], [:like "Tim Duncan"->"Manu Ginobili"]]                | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Tim Duncan"], [:serve "Tim Duncan"->"Spurs"]]                       | ("Spurs")             |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Manu Ginobili"]]                                                    | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"]]             | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Manu Ginobili"], [:serve "Manu Ginobili"->"Spurs"]]                 | ("Spurs")             |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"]]                                                | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"], [:like "LaMarcus Aldridge"->"Tony Parker"]]    | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"], [:like "LaMarcus Aldridge"->"Tim Duncan"]]     | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"], [:serve "LaMarcus Aldridge"->"Trail Blazers"]] | ("Trail Blazers")     |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"], [:serve "LaMarcus Aldridge"->"Spurs"]]         | ("Spurs")             |
      | [[:like "Tim Duncan"->"Tony Parker"], [:serve "Tony Parker"->"Hornets"]]                                                         | ("Hornets")           |
      | [[:like "Tim Duncan"->"Tony Parker"], [:serve "Tony Parker"->"Spurs"]]                                                           | ("Spurs")             |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"]]                                                   | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"], [:like "Tim Duncan"->"Tony Parker"]]              | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"], [:serve "Tim Duncan"->"Spurs"]]                   | ("Spurs")             |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:serve "Manu Ginobili"->"Spurs"]]                                                       | ("Spurs")             |

  Scenario: return all
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:like*2..3]->()
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                             |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Tim Duncan"]]                                                    |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Tim Duncan"], [:like "Tim Duncan"->"Manu Ginobili"]]             |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Manu Ginobili"]]                                                 |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"]]          |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"]]                                             |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"], [:like "LaMarcus Aldridge"->"Tony Parker"]] |
      | [[:like "Tim Duncan"->"Tony Parker"], [:like "Tony Parker"->"LaMarcus Aldridge"], [:like "LaMarcus Aldridge"->"Tim Duncan"]]  |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"]]                                                |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:like "Manu Ginobili"->"Tim Duncan"], [:like "Tim Duncan"->"Tony Parker"]]           |

  Scenario: from one step
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*1]-()
      RETURN e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                           |
      | [[:like "Tim Duncan"->"Manu Ginobili"]]     |
      | [[:like "Tim Duncan"->"Tony Parker"]]       |
      | [[:like "Tim Duncan"<-"Dejounte Murray"]]   |
      | [[:like "Tim Duncan"<-"Shaquile O'Neal"]]   |
      | [[:like "Tim Duncan"<-"Marco Belinelli"]]   |
      | [[:like "Tim Duncan"<-"Boris Diaw"]]        |
      | [[:like "Tim Duncan"<-"Manu Ginobili"]]     |
      | [[:like "Tim Duncan"<-"Danny Green"]]       |
      | [[:like "Tim Duncan"<-"Tiago Splitter"]]    |
      | [[:like "Tim Duncan"<-"Aron Baynes"]]       |
      | [[:like "Tim Duncan"<-"Tony Parker"]]       |
      | [[:like "Tim Duncan"<-"LaMarcus Aldridge"]] |

  Scenario: from one step to one step
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*1..1]-()
      RETURN e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                           |
      | [[:like "Tim Duncan"->"Manu Ginobili"]]     |
      | [[:like "Tim Duncan"->"Tony Parker"]]       |
      | [[:like "Tim Duncan"<-"Dejounte Murray"]]   |
      | [[:like "Tim Duncan"<-"Shaquile O'Neal"]]   |
      | [[:like "Tim Duncan"<-"Marco Belinelli"]]   |
      | [[:like "Tim Duncan"<-"Boris Diaw"]]        |
      | [[:like "Tim Duncan"<-"Manu Ginobili"]]     |
      | [[:like "Tim Duncan"<-"Danny Green"]]       |
      | [[:like "Tim Duncan"<-"Tiago Splitter"]]    |
      | [[:like "Tim Duncan"<-"Aron Baynes"]]       |
      | [[:like "Tim Duncan"<-"Tony Parker"]]       |
      | [[:like "Tim Duncan"<-"LaMarcus Aldridge"]] |

  Scenario: filter by edges
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*2..3]-()
      WHERE e[1].likeness>95 AND e[2].likeness==100
      RETURN e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                         |
      | [[:like "Tim Duncan"<-"Dejounte Murray"], [:like "Dejounte Murray"->"LeBron James"], [:like "LeBron James"->"Ray Allen"]] |

  Scenario: filter by dst node tag
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:teammate*3..4]-(v2:bachelor)
      RETURN DISTINCT v2
      """
    Then the result should be, in any order, with relax comparison:
      | v2                                                                                                          |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

  Scenario: multi-steps and filter by node properties
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e1:like*1..2]-(v2{name: 'Tony Parker'})-[e2:serve]-(v3{name: 'Spurs'})
      RETURN e1, e2
      """
    Then the result should be, in any order, with relax comparison:
      | e1                                                                                      | e2                              |
      | [[:like "Tim Duncan"<-"Dejounte Murray"], [:like "Dejounte Murray"->"Tony Parker"]]     | [:serve "Tony Parker"->"Spurs"] |
      | [[:like "Tim Duncan"->"Manu Ginobili"], [:like "Manu Ginobili"<-"Tony Parker"]]         | [:serve "Tony Parker"->"Spurs"] |
      | [[:like "Tim Duncan"<-"Marco Belinelli"], [:like "Marco Belinelli"->"Tony Parker"]]     | [:serve "Tony Parker"->"Spurs"] |
      | [[:like "Tim Duncan"<-"Boris Diaw"], [:like "Boris Diaw"->"Tony Parker"]]               | [:serve "Tony Parker"->"Spurs"] |
      | [[:like "Tim Duncan"<-"Manu Ginobili"], [:like "Manu Ginobili"<-"Tony Parker"]]         | [:serve "Tony Parker"->"Spurs"] |
      | [[:like "Tim Duncan"<-"LaMarcus Aldridge"], [:like "LaMarcus Aldridge"->"Tony Parker"]] | [:serve "Tony Parker"->"Spurs"] |
      | [[:like "Tim Duncan"<-"LaMarcus Aldridge"], [:like "LaMarcus Aldridge"<-"Tony Parker"]] | [:serve "Tony Parker"->"Spurs"] |
      | [[:like "Tim Duncan"->"Tony Parker"]]                                                   | [:serve "Tony Parker"->"Spurs"] |
      | [[:like "Tim Duncan"<-"Tony Parker"]]                                                   | [:serve "Tony Parker"->"Spurs"] |

  Scenario: Test semantic error for invalid variables
    When executing query:
      """
      MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
      WHERE e[0].likeness>90
      RETURN p
      """
    Then a SemanticError should be raised at runtime: Alias used but not defined: `e'
    When executing query:
      """
      MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
      RETURN e
      """
    Then a SemanticError should be raised at runtime: Alias used but not defined: `e'
    When executing query:
      """
      MATCH p=(v:player{name: 'Tim Duncan'})-[:like|:serve*1..3]->(v1)
      WHERE e[0].likeness+e[1].likeness>90
      RETURN p
      """
    Then a SemanticError should be raised at runtime: Alias used but not defined: `e'

  Scenario: Over expand end
    When executing query:
      """
      MATCH (v:player {name: "Yao Ming"})-[:serve*0..1]->() RETURN v.name
      """
    Then the result should be, in any order:
      | v.name     |
      | "Yao Ming" |
      | "Yao Ming" |
    When executing query:
      """
      MATCH (v:player {name: "Yao Ming"})-[:serve*1..3]->() RETURN v.name
      """
    Then the result should be, in any order:
      | v.name     |
      | "Yao Ming" |
    When executing query:
      """
      MATCH (v:player {name: "Yao Ming"})-[:serve*2..3]->() RETURN v.name
      """
    Then the result should be, in any order:
      | v.name |
    When executing query:
      """
      MATCH (v:player {name: "Yao Ming"})-[:serve*1000000000..1000000002]->() RETURN v.name
      """
    Then the result should be, in any order:
      | v.name |

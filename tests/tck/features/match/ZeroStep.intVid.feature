# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Variable length Pattern match int vid (0 step)

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario Outline: Use step all-direction edge with properties 0 step, return node
    When executing query:
      """
      MATCH <left_node><edge_dir_left><edge><edge_dir_right><right_node>
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                          |
      | ("Tim Duncan":bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

    Examples:
      | left_node                        | edge_dir_left | edge | edge_dir_right | right_node                       |
      | (v :player {name: "Tim Duncan"}) | -             | [*0] | -              | ()                               |
      | (v :player {name: "Tim Duncan"}) | <-            | [*0] | -              | ()                               |
      | (v :player {name: "Tim Duncan"}) | -             | [*0] | ->             | ()                               |
      | ()                               | -             | [*0] | -              | (v :player {name: "Tim Duncan"}) |
      | ()                               | <-            | [*0] | -              | (v :player {name: "Tim Duncan"}) |
      | ()                               | -             | [*0] | ->             | (v :player {name: "Tim Duncan"}) |

  # match (v :player {name: "Tim Duncan"})-[e :like*0..1]-() return e
  Scenario Outline: Use step all-direction edge without properties 0 to 1 step, return edge
    When executing query:
      """
      MATCH <left_node><edge_dir_left><edge><edge_dir_right><right_node>
      RETURN e
      """
    Then the result should be, in any order:
      | e                                                             |
      | [[:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]]       |
      | [[:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]]        |
      | [[:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]]       |
      | [[:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]]   |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]] |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]     |
      | [[:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]]   |
      | [[:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]]  |
      | [[:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]]    |
      | [[:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]]       |
      | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]]     |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]       |
      | []                                                            |
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*0..1]-(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e                                                                      | v                                                                                                           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                                     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

    Examples:
      | left_node                        | edge_dir_left | edge           | edge_dir_right | right_node                       |
      | (v :player {name: "Tim Duncan"}) | -             | [e :like*0..1] | -              | ()                               |
      | ()                               | -             | [e :like*0..1] | -              | (v :player {name: "Tim Duncan"}) |

  # match (v :player {name: "Tim Duncan"})-[e :like*0..1{likeness: 90}]->() return e
  Scenario Outline: Use step one-direction edge with properties 0 to 1 step, return edge
    When executing query:
      """
      MATCH <left_node><edge_dir_left><edge><edge_dir_right><right_node>
      RETURN e
      """
    Then the result should be, in any order:
      | e                                                         |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]] |
      | []                                                        |

    Examples:
      | left_node                        | edge_dir_left | edge                         | edge_dir_right | right_node                       |
      | (v :player {name: "Tim Duncan"}) | <-            | [e :like*0..1{likeness: 90}] | -              | ()                               |
      | ()                               | -             | [e :like*0..1{likeness: 90}] | ->             | (v :player {name: "Tim Duncan"}) |

  Scenario: Edge without properties 0 to n step, return edge and dst
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*0..2]-(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e                                                                                                                                                   | v                                                                                                           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]       | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}]]       | ("Cory Joseph" :player{age: 27, name: "Cory Joseph"})                                                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]]       | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}]]        | ("David West" :player{age: 38, name: "David West"})                                                         |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Dejounte Murray"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]   | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}]]  | ("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})                                             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Kyle Anderson"->"Spurs" @0 {end_year: 2018, start_year: 2014}]]     | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}]]          | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tiago Splitter"->"Spurs" @0 {end_year: 2015, start_year: 2010}]]    | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}]]   | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}]]     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]]                                                                              | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                                                                                                                  | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

  Scenario: Multiple variable length with edge filter
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*0..2]-(v)-[e2 :like*0..2{likeness:90}]-(v2)
      RETURN e2, v2
      """
    Then the result should be, in any order:
      | e2                                                                                                                     | v2                                                                                                          |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]]       | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Vince Carter"->"Tracy McGrady" @0 {likeness: 90}]]     | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}], [:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]] | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | [[:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}], [:like "Yao Ming"->"Shaquille O'Neal" @0 {likeness: 90}]]      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             |
      | [[:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}], [:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]]     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]]         | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             |
      | [[:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}]]       | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}]]      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | [[:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}], [:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}]]      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}], [:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}]]     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}], [:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}]]      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | [[:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}]]                                                                | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]]                                                         | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | [[:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]]                                                          | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]]                                                         | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]]                                                          | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}]]                                                                | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]                                                              | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}]]                                                                | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | [[:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}]]                                                             | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | [[:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}]]                                                              | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | [[:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]]                                                                | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             |
      | [[:like "Vince Carter"->"Tracy McGrady" @0 {likeness: 90}]]                                                            | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | [[:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]]                                                              | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | []                                                                                                                     | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | []                                                                                                                     | ("Cory Joseph" :player{age: 27, name: "Cory Joseph"})                                                       |
      | []                                                                                                                     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | []                                                                                                                     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | []                                                                                                                     | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                                                                                     | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | []                                                                                                                     | ("David West" :player{age: 38, name: "David West"})                                                         |
      | []                                                                                                                     | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | []                                                                                                                     | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | []                                                                                                                     | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | []                                                                                                                     | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | []                                                                                                                     | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | []                                                                                                                     | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | []                                                                                                                     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | []                                                                                                                     | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | []                                                                                                                     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                                                                                     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | []                                                                                                                     | ("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})                                             |
      | []                                                                                                                     | ("Spurs" :team{name: "Spurs"})                                                                              |
    When executing query:
      """
      MATCH (v) -[*0..1]-(v2:player{name: "Tim Duncan"})-[*0..1]->()
      RETURN v, v2
      """
    Then the result should be, in any order:
      | v                                                                                                           | v2                                                                                                          |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Spurs" :team{name: "Spurs"})                                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Spurs" :team{name: "Spurs"})                                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Spurs" :team{name: "Spurs"})                                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Spurs" :team{name: "Spurs"})                                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Spurs" :team{name: "Spurs"})                                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Spurs" :team{name: "Spurs"})                                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Spurs" :team{name: "Spurs"})                                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      MATCH (v) -[*0..1]-(v2:player{name: "Tim Duncan"})-[e*1..2]->(v3)
      RETURN DISTINCT v3.player.name AS pName, v3.team.name as tName, e ORDER BY pName
      """
    Then the result should be, in any order:
      | pName               | tName           | e                                                                                                                                                                          |
      | "Danny Green"       | NULL            | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]]                                                                                            |
      | "Kyle Anderson"     | NULL            | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]]                                  |
      | "Kyle Anderson"     | NULL            | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]]          |
      | "LaMarcus Aldridge" | NULL            | [[:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}]]                                                                                      |
      | "LaMarcus Aldridge" | NULL            | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]]                                                      |
      | "LaMarcus Aldridge" | NULL            | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]]                              |
      | "LaMarcus Aldridge" | NULL            | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}]]                              |
      | "LaMarcus Aldridge" | NULL            | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}]]      |
      | "LeBron James"      | NULL            | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}], [:like "Danny Green"->"LeBron James" @0 {likeness: 80}]]                                   |
      | "Manu Ginobili"     | NULL            | [[:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]]                                                                                          |
      | "Manu Ginobili"     | NULL            | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]]                                                          |
      | "Manu Ginobili"     | NULL            | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]]                                  |
      | "Manu Ginobili"     | NULL            | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]]                                  |
      | "Manu Ginobili"     | NULL            | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]]          |
      | "Manu Ginobili"     | NULL            | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]]                                                                                                                  |
      | "Marco Belinelli"   | NULL            | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}], [:like "Danny Green"->"Marco Belinelli" @0 {likeness: 83}]]                                |
      | "Tim Duncan"        | NULL            | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:teammate "Manu Ginobili"->"Tim Duncan" @0 {end_year: 2016, start_year: 2002}]]                                 |
      | "Tim Duncan"        | NULL            | [[:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}], [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]]                         |
      | "Tim Duncan"        | NULL            | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]]             |
      | "Tim Duncan"        | NULL            | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]]                                     |
      | "Tim Duncan"        | NULL            | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]]                                     |
      | "Tim Duncan"        | NULL            | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}], [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]]                                     |
      | "Tim Duncan"        | NULL            | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]]                                                             |
      | "Tim Duncan"        | NULL            | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]                                                         |
      | "Tim Duncan"        | NULL            | [[:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]                                 |
      | "Tim Duncan"        | NULL            | [[:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}], [:teammate "Manu Ginobili"->"Tim Duncan" @0 {end_year: 2016, start_year: 2002}]]         |
      | "Tony Parker"       | NULL            | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:teammate "Manu Ginobili"->"Tony Parker" @0 {end_year: 2016, start_year: 2002}]]                                |
      | "Tony Parker"       | NULL            | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]                                                                                                                    |
      | "Tony Parker"       | NULL            | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]]                                                                                            |
      | "Tony Parker"       | NULL            | [[:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}], [:teammate "Manu Ginobili"->"Tony Parker" @0 {end_year: 2016, start_year: 2002}]]        |
      | "Tony Parker"       | NULL            | [[:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}], [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]]                        |
      | NULL                | "Cavaliers"     | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}], [:serve "Danny Green"->"Cavaliers" @0 {end_year: 2010, start_year: 2009}]]                 |
      | NULL                | "Spurs"         | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]]                                         |
      | NULL                | "Spurs"         | [[:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}], [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]]                 |
      | NULL                | "Spurs"         | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]]                     |
      | NULL                | "Spurs"         | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]]                                             |
      | NULL                | "Spurs"         | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]]                     |
      | NULL                | "Spurs"         | [[:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}], [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]]         |
      | NULL                | "Hornets"       | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]]                                           |
      | NULL                | "Hornets"       | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}], [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]]                   |
      | NULL                | "Spurs"         | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]]                                                                                                     |
      | NULL                | "Trail Blazers" | [[:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}], [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}]] |
      | NULL                | "Raptors"       | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}], [:serve "Danny Green"->"Raptors" @0 {end_year: 2019, start_year: 2018}]]                   |
    When executing query:
      """
      MATCH (v) -[e1*0..1{likeness: 90}]-(v2:player{name: "Tony Parker"})-[*1..2]->(v3)
      RETURN e1, v3 as Player ORDER BY Player
      """
    Then the result should be, in any order:
      | e1                                                             | Player                                                                                                      |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | []                                                             | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | []                                                             | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | []                                                             | ("Grizzlies" :team{name: "Grizzlies"})                                                                      |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Grizzlies" :team{name: "Grizzlies"})                                                                      |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Hornets" :team{name: "Hornets"})                                                                          |
      | []                                                             | ("Hornets" :team{name: "Hornets"})                                                                          |
      | []                                                             | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | []                                                             | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | []                                                             | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | []                                                             | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | []                                                             | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | []                                                             | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | []                                                             | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | []                                                             | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | []                                                             | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | []                                                             | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | []                                                             | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Spurs" :team{name: "Spurs"})                                                                              |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Spurs" :team{name: "Spurs"})                                                                              |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Spurs" :team{name: "Spurs"})                                                                              |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Spurs" :team{name: "Spurs"})                                                                              |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Spurs" :team{name: "Spurs"})                                                                              |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                             | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                             | ("Trail Blazers" :team{name: "Trail Blazers"})                                                              |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]] | ("Trail Blazers" :team{name: "Trail Blazers"})                                                              |
      | []                                                             | ("Trail Blazers" :team{name: "Trail Blazers"})                                                              |

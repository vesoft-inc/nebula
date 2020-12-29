# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Fix match not filter the undeclared tag, pr#501

  Background:
    Given a graph with space named "nba"

  Scenario: [1] one step given tag without property
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-->(v2:team)
      RETURN v2 AS Team
      """
    Then the result should be, in any order:
      | Team                           |
      | ("Spurs" :team{name: "Spurs"}) |

  Scenario: [2] one step given tag without property
    When executing query:
      """
      MATCH (v:team{name:"Spurs"})--(v2)
      RETURN v2 AS Player
      """
    Then the result should be, in any order:
      | Player                                                                                                      |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | ("Cory Joseph" :player{age: 27, name: "Cory Joseph"})                                                       |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | ("David West" :player{age: 38, name: "David West"})                                                         |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | ("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})                                             |
      | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |

  Scenario: [1] multi steps given tag without property
    When executing query:
      """
      MATCH (v:player{name: "Tim Duncan"})-->(v2:team)<--(v3)
      RETURN v2 AS Team
      """
    Then the result should be, in any order:
      | Team                           |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |

  Scenario: multi steps given tag without property no direction
    When executing query:
      """
      MATCH (v:player{name: "Tim Duncan"})--(v2:team)--(v3)
      RETURN v2 AS Team
      """
    Then the result should be, in any order:
      | Team                           |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |

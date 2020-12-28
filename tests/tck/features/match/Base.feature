# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Basic match

  Scenario: one step
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)
      WHERE v.age >= 38 AND v.age < 45
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name            | Age |
      | 'Paul Gasol'    | 38  |
      | 'Kobe Bryant'   | 40  |
      | 'Vince Carter'  | 42  |
      | 'Tim Duncan'    | 42  |
      | 'Yao Ming'      | 38  |
      | 'Dirk Nowitzki' | 40  |
      | 'Manu Ginobili' | 41  |
      | 'Ray Allen'     | 43  |
      | 'David West'    | 38  |
      | 'Tracy McGrady' | 39  |
    When executing query:
      """
      MATCH (v:player {age: 29})
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: [1] one step given tag without property
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-->(v2:team)
      RETURN v2 AS Team
      """
    Then the result should be, in any order:
      | Team                           |
      | ("Spurs" :team{name: "Spurs"}) |

  @skip
  Scenario: [2] one step given tag without property
    When executing query:
      """
      MATCH (v:team{name:"Spurs"})--(v2)
      RETURN v2 AS Player
      """
    Then the result should be, in any order:
      | ("Player"                                                      ) |
      | ("Boris Diaw" :player{name: "Boris Diaw",age: 36}              ) |
      | ("Kyle Anderson" :player{name: "Kyle Anderson",age: 25}        ) |
      | ("Cory Joseph" :player{name: "Cory Joseph",age: 27}            ) |
      | ("Tiago Splitter" :player{name: "Tiago Splitter",age: 34}      ) |
      | ("LaMarcus Aldridge" :player{name: "LaMarcus Aldridge",age: 33}) |
      | ("Paul Gasol" :player{name: "Paul Gasol",age: 38}              ) |
      | ("Marco Belinelli" :player{name: "Marco Belinelli",age: 32}    ) |
      | ("Tracy McGrady" :player{name: "Tracy McGrady",age: 39}        ) |
      | ("David West" :player{name: "David West",age: 38}              ) |
      | ("Manu Ginobili" :player{name: "Manu Ginobili",age: 41}        ) |
      | ("Tony Parker" :player{name: "Tony Parker",age: 36}            ) |
      | ("Rudy Gay" :player{name: "Rudy Gay",age: 32}                  ) |
      | ("Jonathon Simmons" :player{name: "Jonathon Simmons",age: 29}  ) |
      | ("Aron Baynes" :player{name: "Aron Baynes",age: 32}            ) |
      | ("Danny Green" :player{name: "Danny Green",age: 31}            ) |
      | ("Tim Duncan" :player{name: "Tim Duncan",age: 42}              ) |
      | ("Marco Belinelli" :player{name: "Marco Belinelli",age: 32}    ) |
      | ("Dejounte Murray" :player{name: "Dejounte Murray",age: 29}    ) |

  @skip
  Scenario: [1] multi steps given tag without property
    Given a graph with space named "nba"
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

  @skip
  Scenario: multi steps given tag without property no direction
    Given a graph with space named "nba"
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

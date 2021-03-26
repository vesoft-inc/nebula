# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: test zero steps pattern
  Examples:
    | space_name  |
    | nba         |
    | nba_int_vid |

  Background:
    Given a graph with space named "<space_name>"

  Scenario Outline: Test some boundary usages
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*0]-()
      RETURN e
      """
    Then the result should be, in any order, with relax comparison:
      | e  |
      | [] |
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..0]-()
      RETURN e
      """
    Then the result should be, in any order, with relax comparison:
      | e  |
      | [] |
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*]-()
      RETURN e
      """
    Then a SemanticError should be raised at runtime: Cannot set maximum hop for variable length relationships
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..0]-()-[e2:like*0..0]-()
      RETURN e, e2
      """
    Then the result should be, in any order, with relax comparison:
      | e  | e2 |
      | [] | [] |

  Scenario Outline: Test mix query of fixed and variable hops
    When executing query:
      """
      MATCH (:player{name: "Tim Duncan"})-[e1:like]->()-[e2:serve*0..3]->()<-[e3:serve]-(v)
      RETURN count(v)
      """
    Then the result should be, in any order, with relax comparison:
      | count(v) |
      | 40       |

  Scenario Outline: Test return all variables
    When executing query:
      """
      MATCH (v:player{name: "abc"}) -[:serve*1..3]-> ()
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      MATCH (v:player{name: "abc"}) -[:serve*..3]-> ()
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      MATCH (v:player{name: "abc"}) -[:serve*1..]-> ()
      RETURN *
      """
    Then a SemanticError should be raised at runtime: Cannot set maximum hop for variable length relationships

  Scenario Outline: Single edge with properties in both directions
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:serve*0..1{start_year: 2000}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                   | v                 |
      | []                                  | ('Tracy McGrady') |
      | [[:serve 'Tracy McGrady'->'Magic']] | ('Magic')         |
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1{likeness: 90}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                         | v                 |
      | []                                        | ('Tracy McGrady') |
      | [[:like 'Tracy McGrady'->'Kobe Bryant']]  | ('Kobe Bryant')   |
      | [[:like 'Tracy McGrady'->'Grant Hill']]   | ('Grant Hill')    |
      | [[:like 'Tracy McGrady'->'Rudy Gay']]     | ('Rudy Gay')      |
      | [[:like 'Tracy McGrady'<-'Vince Carter']] | ('Vince Carter')  |
      | [[:like 'Tracy McGrady'<-'Yao Ming']]     | ('Yao Ming')      |
      | [[:like 'Tracy McGrady'<-'Grant Hill']]   | ('Grant Hill')    |
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:like*1{likeness: 90}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                         | v                |
      | [[:like 'Tracy McGrady'->'Kobe Bryant']]  | ('Kobe Bryant')  |
      | [[:like 'Tracy McGrady'->'Grant Hill']]   | ('Grant Hill')   |
      | [[:like 'Tracy McGrady'->'Rudy Gay']]     | ('Rudy Gay')     |
      | [[:like 'Tracy McGrady'<-'Vince Carter']] | ('Vince Carter') |
      | [[:like 'Tracy McGrady'<-'Yao Ming']]     | ('Yao Ming')     |
      | [[:like 'Tracy McGrady'<-'Grant Hill']]   | ('Grant Hill')   |
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:like*0{likeness: 90}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e  | v                 |
      | [] | ('Tracy McGrady') |

  Scenario Outline: Single edge with properties in single directions
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1{likeness: 90}]->(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                        | v                 |
      | []                                       | ('Tracy McGrady') |
      | [[:like 'Tracy McGrady'->'Kobe Bryant']] | ('Kobe Bryant')   |
      | [[:like 'Tracy McGrady'->'Grant Hill']]  | ('Grant Hill')    |
      | [[:like 'Tracy McGrady'->'Rudy Gay']]    | ('Rudy Gay')      |
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:like*0{likeness: 90}]->(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e  | v                 |
      | [] | ('Tracy McGrady') |
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:like*1{likeness: 90}]->(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                        | v               |
      | [[:like 'Tracy McGrady'->'Kobe Bryant']] | ('Kobe Bryant') |
      | [[:like 'Tracy McGrady'->'Grant Hill']]  | ('Grant Hill')  |
      | [[:like 'Tracy McGrady'->'Rudy Gay']]    | ('Rudy Gay')    |

  Scenario Outline: Single edge without properties in both directions
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:serve*0..1]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                     | v                 |
      | []                                    | ('Tracy McGrady') |
      | [[:serve 'Tracy McGrady'->'Raptors']] | ('Raptors')       |
      | [[:serve 'Tracy McGrady'->'Magic']]   | ('Magic')         |
      | [[:serve 'Tracy McGrady'->'Spurs']]   | ('Spurs')         |
      | [[:serve 'Tracy McGrady'->'Rockets']] | ('Rockets')       |
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                         | v                 |
      | []                                        | ('Tracy McGrady') |
      | [[:like 'Tracy McGrady'->'Kobe Bryant']]  | ('Kobe Bryant')   |
      | [[:like 'Tracy McGrady'->'Grant Hill']]   | ('Grant Hill')    |
      | [[:like 'Tracy McGrady'->'Rudy Gay']]     | ('Rudy Gay')      |
      | [[:like 'Tracy McGrady'<-'Vince Carter']] | ('Vince Carter')  |
      | [[:like 'Tracy McGrady'<-'Yao Ming']]     | ('Yao Ming')      |
      | [[:like 'Tracy McGrady'<-'Grant Hill']]   | ('Grant Hill')    |

  Scenario Outline: Multiple edges with properties in both directions
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{start_year: 2000}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                   | v                 |
      | []                                  | ('Tracy McGrady') |
      | [[:serve 'Tracy McGrady'->'Magic']] | ('Magic')         |

  Scenario Outline: Multiple edges with properties in single direction
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{start_year: 2000}]->(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                   | v                 |
      | []                                  | ('Tracy McGrady') |
      | [[:serve 'Tracy McGrady'->'Magic']] | ('Magic')         |
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{likeness: 90}]->(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                        | v                 |
      | []                                       | ('Tracy McGrady') |
      | [[:like 'Tracy McGrady'->'Kobe Bryant']] | ('Kobe Bryant')   |
      | [[:like 'Tracy McGrady'->'Grant Hill']]  | ('Grant Hill')    |
      | [[:like 'Tracy McGrady'->'Rudy Gay']]    | ('Rudy Gay')      |

  Scenario Outline: Multiple edges without properties in both directions
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                         | v                 |
      | []                                        | ('Tracy McGrady') |
      | [[:like 'Tracy McGrady'->'Kobe Bryant']]  | ('Kobe Bryant')   |
      | [[:like 'Tracy McGrady'->'Grant Hill']]   | ('Grant Hill')    |
      | [[:like 'Tracy McGrady'->'Rudy Gay']]     | ('Rudy Gay')      |
      | [[:like 'Tracy McGrady'<-'Vince Carter']] | ('Vince Carter')  |
      | [[:like 'Tracy McGrady'<-'Yao Ming']]     | ('Yao Ming')      |
      | [[:like 'Tracy McGrady'<-'Grant Hill']]   | ('Grant Hill')    |
      | [[:serve 'Tracy McGrady'->'Raptors']]     | ('Raptors')       |
      | [[:serve 'Tracy McGrady'->'Magic']]       | ('Magic')         |
      | [[:serve 'Tracy McGrady'->'Spurs']]       | ('Spurs')         |
      | [[:serve 'Tracy McGrady'->'Rockets']]     | ('Rockets')       |

  Scenario Outline: Multiple edges without properties in single direction
    When executing query:
      """
      MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1]->(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                        | v                 |
      | []                                       | ('Tracy McGrady') |
      | [[:like 'Tracy McGrady'->'Kobe Bryant']] | ('Kobe Bryant')   |
      | [[:like 'Tracy McGrady'->'Grant Hill']]  | ('Grant Hill')    |
      | [[:like 'Tracy McGrady'->'Rudy Gay']]    | ('Rudy Gay')      |
      | [[:serve 'Tracy McGrady'->'Raptors']]    | ('Raptors')       |
      | [[:serve 'Tracy McGrady'->'Magic']]      | ('Magic')         |
      | [[:serve 'Tracy McGrady'->'Spurs']]      | ('Spurs')         |
      | [[:serve 'Tracy McGrady'->'Rockets']]    | ('Rockets')       |

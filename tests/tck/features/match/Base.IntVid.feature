# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Basic match

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Single node
    When executing query:
      """
      MATCH (v:player {name: "Yao Ming"}) RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v            |
      | ("Yao Ming") |
    When executing query:
      """
      MATCH (v:player) WHERE v.name == "Yao Ming" RETURN v.age AS Age
      """
    Then the result should be, in any order:
      | Age |
      | 38  |
    When executing query:
      """
      MATCH (v:player {age: 29}) return v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    When executing query:
      """
      MATCH (v:player {age: 29}) WHERE v.name STARTS WITH "J" return v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
    When executing query:
      """
      MATCH (v:player) WHERE v.age >= 38 AND v.age < 45 return v.name AS Name, v.age AS Age
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
      MATCH (v:player) where v.age > 9223372036854775807+1  return v
      """
    Then a ExecutionError should be raised at runtime: result of (9223372036854775807+1) cannot be represented as an integer
    When executing query:
      """
      MATCH (v:player) where v.age > -9223372036854775808-1  return v
      """
    Then a ExecutionError should be raised at runtime: result of (-9223372036854775808-1) cannot be represented as an integer

  Scenario: Une step
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r]-> (v2) RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order:
      | Type    | Name        |
      | "like"  | "Ray Allen" |
      | "serve" | "Cavaliers" |
      | "serve" | "Heat"      |
      | "serve" | "Lakers"    |
      | "serve" | "Cavaliers" |
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r:serve|:like]-> (v2) RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order:
      | Type    | Name        |
      | "serve" | "Cavaliers" |
      | "serve" | "Heat"      |
      | "serve" | "Lakers"    |
      | "serve" | "Cavaliers" |
      | "like"  | "Ray Allen" |
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2)
      RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order:
      | Type    | Name        |
      | "serve" | "Cavaliers" |
      | "serve" | "Heat"      |
      | "serve" | "Lakers"    |
      | "serve" | "Cavaliers" |
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2 {name: "Cavaliers"})
      RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order:
      | Type    | Name        |
      | "serve" | "Cavaliers" |
      | "serve" | "Cavaliers" |
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2 {name: "Cavaliers"})
      WHERE r.start_year <= 2005 AND r.end_year >= 2005
      RETURN r.start_year AS Start_Year, r.end_year AS Start_Year
      """
    Then the result should be, in any order:
      | Start_Year | Start_Year |
      | 2003       | 2010       |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) -[:like]-> (v2)
      RETURN v1.name AS Name, v2.name AS Friend
      """
    Then the result should be, in any order:
      | Name          | Friend            |
      | "Danny Green" | "LeBron James"    |
      | "Danny Green" | "Marco Belinelli" |
      | "Danny Green" | "Tim Duncan"      |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) <-[:like]- (v2)
      RETURN v1.name AS Name, v2.name AS Friend
      """
    Then the result should be, in any order:
      | Name          | Friend            |
      | "Danny Green" | "Dejounte Murray" |
      | "Danny Green" | "Marco Belinelli" |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) <-[:like]-> (v2)
      RETURN v1.name AS Name, v2.name AS Friend
      """
    Then the result should be, in any order:
      | Name          | Friend            |
      | "Danny Green" | "Dejounte Murray" |
      | "Danny Green" | "Marco Belinelli" |
      | "Danny Green" | "LeBron James"    |
      | "Danny Green" | "Marco Belinelli" |
      | "Danny Green" | "Tim Duncan"      |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) -[:like]- (v2)
      RETURN v1.name AS Name, v2.name AS Friend
      """
    Then the result should be, in any order:
      | Name          | Friend            |
      | "Danny Green" | "Dejounte Murray" |
      | "Danny Green" | "Marco Belinelli" |
      | "Danny Green" | "LeBron James"    |
      | "Danny Green" | "Marco Belinelli" |
      | "Danny Green" | "Tim Duncan"      |

  Scenario: two steps
    When executing query:
      """
      MATCH (v1:player{age: 28}) -[:like]-> (v2) -[:like]-> (v3)
      RETURN v1.name AS Player, v2.name AS Friend, v3.name AS FoF
      """
    Then the result should be, in any order:
      | Player           | Friend              | FoF            |
      | "Paul George"    | "Russell Westbrook" | "James Harden" |
      | "Paul George"    | "Russell Westbrook" | "Paul George"  |
      | "Damian Lillard" | "LaMarcus Aldridge" | "Tim Duncan"   |
      | "Damian Lillard" | "LaMarcus Aldridge" | "Tony Parker"  |
    When executing query:
      """
      MATCH (v1:player{name: 'Tony Parker'}) -[r1:serve]-> (v2) <-[r2:serve]- (v3)
      WHERE r1.start_year <= r2.end_year AND
            r1.end_year >= r2.start_year AND
            v1.name <> v3.name AND
            v3.name STARTS WITH 'D'
      RETURN v1.name AS Player, v2.name AS Team, v3.name AS Teammate
      """
    Then the result should be, in any order:
      | Player        | Team      | Teammate          |
      | "Tony Parker" | "Hornets" | "Dwight Howard"   |
      | "Tony Parker" | "Spurs"   | "Danny Green"     |
      | "Tony Parker" | "Spurs"   | "David West"      |
      | "Tony Parker" | "Spurs"   | "Dejounte Murray" |

  Scenario: Uistinct
    When executing query:
      """
      MATCH (:player{name:'Dwyane Wade'}) -[:like]-> () -[:like]-> (v3)
      RETURN v3.name AS Name
      """
    Then the result should be, in any order:
      | Name              |
      | "Carmelo Anthony" |
      | "Dwyane Wade"     |
      | "LeBron James"    |
      | "Chris Paul"      |
      | "Dwyane Wade"     |
      | "LeBron James"    |
      | "Ray Allen"       |
    When executing query:
      """
      MATCH (:player{name:'Dwyane Wade'}) -[:like]-> () -[:like]-> (v3)
      RETURN DISTINCT v3.name AS Name
      """
    Then the result should be, in any order:
      | Name              |
      | "Carmelo Anthony" |
      | "Dwyane Wade"     |
      | "LeBron James"    |
      | "Chris Paul"      |
      | "Ray Allen"       |

  Scenario: Order skip limit
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      """
    Then the result should be, in any order:
      | Name                | Age |
      | "Tim Duncan"        | 42  |
      | "Manu Ginobili"     | 41  |
      | "Tony Parker"       | 36  |
      | "LeBron James"      | 34  |
      | "Chris Paul"        | 33  |
      | "Marco Belinelli"   | 32  |
      | "Danny Green"       | 31  |
      | "Kevin Durant"      | 30  |
      | "Russell Westbrook" | 30  |
      | "James Harden"      | 29  |
      | "Kyle Anderson"     | 25  |
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name            | Age |
      | "Tim Duncan"    | 42  |
      | "Manu Ginobili" | 41  |
      | "Tony Parker"   | 36  |
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      SKIP 3
      """
    Then the result should be, in any order:
      | Name                | Age |
      | "LeBron James"      | 34  |
      | "Chris Paul"        | 33  |
      | "Marco Belinelli"   | 32  |
      | "Danny Green"       | 31  |
      | "Kevin Durant"      | 30  |
      | "Russell Westbrook" | 30  |
      | "James Harden"      | 29  |
      | "Kyle Anderson"     | 25  |
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      SKIP 3
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name              | Age |
      | "LeBron James"    | 34  |
      | "Chris Paul"      | 33  |
      | "Marco Belinelli" | 32  |
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      SKIP 11
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name | Age |
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      LIMIT 0
      """
    Then the result should be, in any order:
      | Name | Age |

  Scenario: Order by vertex prop
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.name AS Name, v.age AS Age
      ORDER BY v.age DESC, v.name ASC
      """
    Then a SemanticError should be raised at runtime: Only column name can be used as sort item

  Scenario: Return path
    When executing query:
      """
      MATCH p = (n:player{name:"Tony Parker"}) return p,n
      """
    Then the result should be, in any order, with relax comparison:
      | p                 | n               |
      | <("Tony Parker")> | ("Tony Parker") |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m) return p, n.name, m.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                           | n.name         | m.name      |
      | <("LeBron James")-[:like@0]->("Ray Allen")> | "LeBron James" | "Ray Allen" |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m) return p, n.name, m.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                 | n.name         | m.name            |
      | <("LeBron James")<-[:like@0]-("Carmelo Anthony")> | "LeBron James" | "Carmelo Anthony" |
      | <("LeBron James")<-[:like@0]-("Chris Paul")>      | "LeBron James" | "Chris Paul"      |
      | <("LeBron James")<-[:like@0]-("Danny Green")>     | "LeBron James" | "Danny Green"     |
      | <("LeBron James")<-[:like@0]-("Dejounte Murray")> | "LeBron James" | "Dejounte Murray" |
      | <("LeBron James")<-[:like@0]-("Dwyane Wade")>     | "LeBron James" | "Dwyane Wade"     |
      | <("LeBron James")<-[:like@0]-("Kyrie Irving")>    | "LeBron James" | "Kyrie Irving"    |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]-(m) return p, n.name, m.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                 | n.name         | m.name            |
      | <("LeBron James")<-[:like@0]-("Carmelo Anthony")> | "LeBron James" | "Carmelo Anthony" |
      | <("LeBron James")<-[:like@0]-("Chris Paul")>      | "LeBron James" | "Chris Paul"      |
      | <("LeBron James")<-[:like@0]-("Danny Green")>     | "LeBron James" | "Danny Green"     |
      | <("LeBron James")<-[:like@0]-("Dejounte Murray")> | "LeBron James" | "Dejounte Murray" |
      | <("LeBron James")<-[:like@0]-("Dwyane Wade")>     | "LeBron James" | "Dwyane Wade"     |
      | <("LeBron James")<-[:like@0]-("Kyrie Irving")>    | "LeBron James" | "Kyrie Irving"    |
      | <("LeBron James")-[:like@0]->("Ray Allen")>       | "LeBron James" | "Ray Allen"       |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)-[:like]->(k) return p, n.name, m.name, k.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                      | n.name         | m.name      | k.name        |
      | <("LeBron James")-[:like@0]->("Ray Allen")-[:like@0]->("Rajon Rondo")> | "LeBron James" | "Ray Allen" | "Rajon Rondo" |
    When executing query:
      """
      MATCH p=(:player{name:"LeBron James"})-[:like]->()-[:like]->() RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                      |
      | <("LeBron James")-[:like@0]->("Ray Allen")-[:like@0]->("Rajon Rondo")> |

  Scenario: exists
    When executing query:
      """
      MATCH (:player{name:"Tony Parker"})-[r]->() where exists(r.likeness) return r, exists({a:12}.a)
      """
    Then the result should be, in any order, with relax comparison:
      | r                                                            | exists({a:12}.a) |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] | true             |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     | true             |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        | true             |
    When executing query:
      """
      MATCH (:player{name:"Tony Parker"})-[r]->(m) where exists(m.likeness) return r, exists({a:12}.a)
      """
    Then the result should be, in any order, with relax comparison:
      | r | exists({a:12}.a) |
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->(m) where exists({abc:123}.abc) return r
      """
    Then the result should be, in any order, with relax comparison:
      | r                                                                                    |
      | [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]     |
      | [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}] |
      | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]     |
      | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]        |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]                         |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                                |
      | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]              |
      | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]                |
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->(m) where exists({abc:123}.ab) return r
      """
    Then the result should be, in any order, with relax comparison:
      | r |
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->(m) where exists(m.age) return r
      """
    Then the result should be, in any order, with relax comparison:
      | r                                                                                    |
      | [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]     |
      | [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}] |
      | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]     |
      | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]        |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]                         |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                                |

  Scenario: No return
    When executing query:
      """
      MATCH (v:player{name:"abc"})
      """
    Then a SyntaxError should be raised at runtime: syntax error near `)'

  Scenario: Unimplemented features
    When executing query:
      """
      MATCH (v) return v
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH (v) RETURN v
    When executing query:
      """
      MATCH (v{name: "Tim Duncan"}) return v
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH (v{name:"Tim Duncan"}) RETURN v
    When executing query:
      """
      MATCH (v:player:bachelor) RETURN v
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH (v:player:bachelor) RETURN v
    When executing query:
      """
      MATCH (v:player{age:23}:bachelor) RETURN v
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH (v:player{age:23}:bachelor) RETURN v
    When executing query:
      """
      MATCH () -[]-> (v) return *
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH ()-->(v) RETURN *
    When executing query:
      """
      MATCH () --> (v) --> () return *
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH ()-->(v)-->() RETURN *
    # The 0 step means node scan in fact, but p and t has no label or properties for index seek
    # So it's not workable now
    When executing query:
      """
      MATCH (p)-[:serve*0..3]->(t) RETURN p
      """
    Then a SemanticError should be raised at runtime: Can't solve the start vids from the sentence: MATCH (p)-[:serve*0..3]->(t) RETURN p

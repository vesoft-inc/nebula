# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Basic match

  Background:
    Given a graph with space named "nba"

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
      MATCH (v:player) WHERE v.player.age < 0 RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                      |
      | ("Null1" :player{age: -1, name: NULL}) |
      | ("Null2" :player{age: -2, name: NULL}) |
      | ("Null3" :player{age: -3, name: NULL}) |
      | ("Null4" :player{age: -4, name: NULL}) |
    When executing query:
      """
      MATCH (v:player) WHERE v.player.name == "Yao Ming" RETURN v.player.age AS Age
      """
    Then the result should be, in any order:
      | Age |
      | 38  |
    When executing query:
      """
      MATCH (v:player {age: 29}) return v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    When executing query:
      """
      MATCH (v:player {age: 29}) WHERE v.player.name STARTS WITH "J" return v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
    When executing query:
      """
      MATCH (v:player) WHERE v.player.age >= 38 AND v.player.age < 45 return v.player.name AS Name, v.player.age AS Age
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
      MATCH (v:player) where v.player.name == null RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      MATCH (v:player) where v.player.name == 3 RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      MATCH (v:player{name: "Tim Duncan"}) RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                                                                           |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      MATCH (v:player) where v.player.age == 38 RETURN *, v.player.age + 100 AS age
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                   | age |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"}) | 138 |
      | ("David West" :player{age: 38, name: "David West"}) | 138 |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})     | 138 |
    When executing query:
      """
      MATCH (v:player) where v.player.age > 9223372036854775807+1  return v
      """
    Then a SemanticError should be raised at runtime: result of (9223372036854775807+1) cannot be represented as an integer
    When executing query:
      """
      MATCH (v:player) where v.player.age > -9223372036854775808-1  return v
      """
    Then a SemanticError should be raised at runtime: result of (-9223372036854775808-1) cannot be represented as an integer

  Scenario: One step
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r]-> (v2)
      RETURN type(r) AS Type, CASE WHEN v2.tea.name IS NOT NULL THEN v2.tea.name WHEN v2.playe.name IS NOT NULL THEN v2.playe.name ELSE "abc" END AS Name
      """
    Then the result should be, in any order:
      | Type    | Name  |
      | "like"  | "abc" |
      | "serve" | "abc" |
      | "serve" | "abc" |
      | "serve" | "abc" |
      | "serve" | "abc" |
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r]-> (v2)
      RETURN type(r) AS Type, CASE WHEN v2.tea.name IS NOT NULL THEN v2.tea.name WHEN v2.playe.name IS NOT NULL THEN v2.playe.name END AS Name
      """
    Then the result should be, in any order:
      | Type    | Name |
      | "like"  | NULL |
      | "serve" | NULL |
      | "serve" | NULL |
      | "serve" | NULL |
      | "serve" | NULL |
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r]-> (v2)
      RETURN type(r) AS Type, CASE WHEN v2.team.name IS NOT NULL THEN v2.team.name WHEN v2.player.name IS NOT NULL THEN v2.player.name END AS Name
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
      MATCH (v1:player{name: "LeBron James"}) -[r:serve|:like]-> (v2)
      RETURN type(r) AS Type, CASE WHEN v2.team.name IS NOT NULL THEN v2.team.name WHEN v2.player.name IS NOT NULL THEN v2.player.name END AS Name
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
      RETURN type(r) AS Type, v2.team.name AS Name
      """
    Then the result should be, in any order:
      | Type    | Name        |
      | "serve" | "Cavaliers" |
      | "serve" | "Heat"      |
      | "serve" | "Lakers"    |
      | "serve" | "Cavaliers" |
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2:team{name: "Cavaliers"})
      RETURN type(r) AS Type, v2.team.name AS Name
      """
    Then the result should be, in any order:
      | Type    | Name        |
      | "serve" | "Cavaliers" |
      | "serve" | "Cavaliers" |
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2:team{name: "Cavaliers"})
      WHERE r.start_year <= 2005 AND r.end_year >= 2005
      RETURN r.start_year AS Start_Year, r.end_year AS Start_Year
      """
    Then the result should be, in any order:
      | Start_Year | Start_Year |
      | 2003       | 2010       |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) -[:like]-> (v2)
      RETURN v1.player.name AS Name, v2.player.name AS Friend
      """
    Then the result should be, in any order:
      | Name          | Friend            |
      | "Danny Green" | "LeBron James"    |
      | "Danny Green" | "Marco Belinelli" |
      | "Danny Green" | "Tim Duncan"      |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) <-[:like]- (v2)
      RETURN v1.player.name AS Name, v2.player.name AS Friend
      """
    Then the result should be, in any order:
      | Name          | Friend            |
      | "Danny Green" | "Dejounte Murray" |
      | "Danny Green" | "Marco Belinelli" |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) <-[:like]-> (v2)
      RETURN v1.player.name AS Name, v2.player.name AS Friend
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
      RETURN v1.player.name AS Name, v2.player.name AS Friend
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
      MATCH (v:player)-[e:like]-(v2) where v.player.age == 38 RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                   | e                                                        | v2                                                              |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"}) | [:like "Marc Gasol"->"Paul Gasol" @0 {likeness: 99}]     | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"})             |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"}) | [:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}]    | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})           |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"}) | [:like "Paul Gasol"->"Marc Gasol" @0 {likeness: 99}]     | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"})             |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})     | [:like "Yao Ming"->"Shaquille O'Neal" @0 {likeness: 90}] | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"}) |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})     | [:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]    | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})       |
    When executing query:
      """
      MATCH (v:player)-[e:like]->(v2) where id(v) == "Tim Duncan" RETURN DISTINCT properties(e) as props, e
      """
    Then the result should be, in any order, with relax comparison:
      | props          | e                                                       |
      | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    When executing query:
      """
      MATCH (v:player)-[e:like]->(v2) where id(v) == "Tim Duncan" RETURN DISTINCT properties(e) as props
      """
    Then the result should be, in any order, with relax comparison:
      | props          |
      | {likeness: 95} |

  Scenario: two steps
    When executing query:
      """
      MATCH (v1:player{age: 28}) -[:like]-> (v2) -[:like]-> (v3)
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
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
            v1.player.name <> v3.player.name AND
            v3.player.name STARTS WITH 'D'
      RETURN v1.player.name AS Player, v2.team.name AS Team, v3.player.name AS Teammate
      """
    Then the result should be, in any order:
      | Player        | Team      | Teammate          |
      | "Tony Parker" | "Hornets" | "Dwight Howard"   |
      | "Tony Parker" | "Spurs"   | "Danny Green"     |
      | "Tony Parker" | "Spurs"   | "David West"      |
      | "Tony Parker" | "Spurs"   | "Dejounte Murray" |

  Scenario: Distinct
    When executing query:
      """
      MATCH (:player{name:'Dwyane Wade'}) -[:like]-> () -[:like]-> (v3)
      RETURN v3.player.name AS Name
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
      RETURN DISTINCT v3.player.name AS Name
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
      RETURN v.player.name AS Name, v.player.age AS Age
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
      RETURN v.player.name AS Name, v.player.age AS Age
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
      RETURN v.player.name AS Name, v.player.age AS Age
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
      RETURN v.player.name AS Name, v.player.age AS Age
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
      RETURN v.player.name AS Name, v.player.age AS Age
      ORDER BY Age DESC, Name ASC
      SKIP 11
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name | Age |
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.player.name AS Name, v.player.age AS Age
      ORDER BY Age DESC, Name ASC
      LIMIT 0
      """
    Then the result should be, in any order:
      | Name | Age |

  Scenario: Order by vertex prop
    When executing query:
      """
      MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
      RETURN v.player.name AS Name, v.player.age AS Age
      ORDER BY v.player.age DESC, v.player.name ASC
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
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m) return p, n.player.name, m.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                           | n.player.name  | m.player.name |
      | <("LeBron James")-[:like@0]->("Ray Allen")> | "LeBron James" | "Ray Allen"   |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m) return p, n.player.name, m.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                 | n.player.name  | m.player.name     |
      | <("LeBron James")<-[:like@0]-("Carmelo Anthony")> | "LeBron James" | "Carmelo Anthony" |
      | <("LeBron James")<-[:like@0]-("Chris Paul")>      | "LeBron James" | "Chris Paul"      |
      | <("LeBron James")<-[:like@0]-("Danny Green")>     | "LeBron James" | "Danny Green"     |
      | <("LeBron James")<-[:like@0]-("Dejounte Murray")> | "LeBron James" | "Dejounte Murray" |
      | <("LeBron James")<-[:like@0]-("Dwyane Wade")>     | "LeBron James" | "Dwyane Wade"     |
      | <("LeBron James")<-[:like@0]-("Kyrie Irving")>    | "LeBron James" | "Kyrie Irving"    |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]-(m) return p, n.player.name, m.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                 | n.player.name  | m.player.name     |
      | <("LeBron James")<-[:like@0]-("Carmelo Anthony")> | "LeBron James" | "Carmelo Anthony" |
      | <("LeBron James")<-[:like@0]-("Chris Paul")>      | "LeBron James" | "Chris Paul"      |
      | <("LeBron James")<-[:like@0]-("Danny Green")>     | "LeBron James" | "Danny Green"     |
      | <("LeBron James")<-[:like@0]-("Dejounte Murray")> | "LeBron James" | "Dejounte Murray" |
      | <("LeBron James")<-[:like@0]-("Dwyane Wade")>     | "LeBron James" | "Dwyane Wade"     |
      | <("LeBron James")<-[:like@0]-("Kyrie Irving")>    | "LeBron James" | "Kyrie Irving"    |
      | <("LeBron James")-[:like@0]->("Ray Allen")>       | "LeBron James" | "Ray Allen"       |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)-[:like]->(k) return p, n.player.name, m.player.name, k.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                      | n.player.name  | m.player.name | k.player.name |
      | <("LeBron James")-[:like@0]->("Ray Allen")-[:like@0]->("Rajon Rondo")> | "LeBron James" | "Ray Allen"   | "Rajon Rondo" |
    When executing query:
      """
      MATCH p=(:player{name:"LeBron James"})-[:like]->()-[:like]->() RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                      |
      | <("LeBron James")-[:like@0]->("Ray Allen")-[:like@0]->("Rajon Rondo")> |

  Scenario: Match a path in a space which doesn't have edge schema
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string);
      """
    When try to execute query:
      """
      INSERT VERTEX person VALUES "Tom":("Tom")
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH p=(v)-[e*1]->(v2) WHERE id(v) IN ["Tom"] RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p |

  @skip
  Scenario: Unsupported combination of some cypher clauses
    When executing query:
      """
      MATCH (v:player) MATCH (t:team) RETURN v, t
      """
    Then a SemanticError should be raised at runtime: Match clause is not supported to be followed by other cypher clauses
    When executing query:
      """
      UNWIND ["Tony Parker", "Tim Duncan", "Yao Ming"] AS a MATCH (v:player) RETURN a, v
      """
    Then a SemanticError should be raised at runtime: Match clause is not supported to be followed by other cypher clauses
    When executing query:
      """
      WITH "Tony Parker" AS a MATCH (v:player) RETURN a, v
      """
    Then a SemanticError should be raised at runtime: Match clause is not supported to be followed by other cypher clauses

  Scenario: exists
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->() where exists(r.likeness) return r, exists({a:12}.a)
      """
    Then the result should be, in any order, with relax comparison:
      | r                                                            | exists({a:12}.a) |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] | true             |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     | true             |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        | true             |
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->(m) where exists(m.player.likeness) return r, exists({a:12}.a)
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
      match (:player{name:"Tony Parker"})-[r]->(m) where exists(m.player.age) return r
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

  Scenario: filter evaluable
    When executing query:
      """
      match (v:player{age: -1}) return v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                      |
      | ("Null1" :player{age: -1, name: NULL}) |
    When executing query:
      """
      match (v:player{age: +20}) return v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                     |
      | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"}) |
    When executing query:
      """
      match (v:player{age: 1+19}) return v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                     |
      | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"}) |
    When executing query:
      """
      match (v:player)-[e:like{likeness:-1}]->()  return e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                       |
      | [:like "Blake Griffin"->"Chris Paul" @0 {likeness: -1}] |
      | [:like "Rajon Rondo"->"Ray Allen" @0 {likeness: -1}]    |
    When executing query:
      """
      match (v:player)-[e:like{likeness:40+50+5}]->()  return e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                            |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | [:like "Paul George"->"Russell Westbrook" @0 {likeness: 95}] |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    When executing query:
      """
      match (v:player)-[e:like{likeness:4*20+5}]->()  return e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                    |
      | [:like "Jason Kidd"->"Dirk Nowitzki"@0{likeness:85}] |
      | [:like "Steve Nash"->"Jason Kidd"@0{likeness:85}]    |
    When executing query:
      """
      match (v:player)-[e:like{likeness:"99"}]->()  return e
      """
    Then the result should be, in any order, with relax comparison:
      | e |
    When executing query:
      """
      match (v:player{age:"24"-1})  return v
      """
    Then a SemanticError should be raised at runtime: Type error `("24"-1)'

  Scenario: No return
    When executing query:
      """
      MATCH (v:player{name:"abc"})
      """
    Then a SyntaxError should be raised at runtime: syntax error near `)'
    When executing query:
      """
      MATCH (v:player) where v.player.name return v
      """
    Then a ExecutionError should be raised at runtime: Wrong type result, the type should be NULL, EMPTY, BOOL

  Scenario: Unimplemented features
    When executing query:
      """
      MATCH (v) return v
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v:player:bachelor) RETURN v
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v:player{age:23}:bachelor) RETURN v
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH () -[]-> (v) return *
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH () --> (v) --> () return *
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    # The 0 step means node scan in fact, but p and t has no label or properties for index seek
    # So it's not workable now
    When executing query:
      """
      MATCH (p)-[:serve*0..3]->(t) RETURN p
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: can't get property of vertex
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"}) return v.name
      """
    Then a SemanticError should be raised at runtime: To get the property of the vertex in `v.name', should use the format `var.tag.prop'
    When executing query:
      """
      MATCH (v:player)-[]->(b) WHERE v.age > 30 return v.player.name
      """
    Then a SemanticError should be raised at runtime: To get the property of the vertex in `v.age', should use the format `var.tag.prop'
    When executing query:
      """
      MATCH (v:player)-[:like]->(b) WHERE v.player.age > 30 return v.player.name, b.age
      """
    Then a SemanticError should be raised at runtime: To get the property of the vertex in `b.age', should use the format `var.tag.prop'
    When executing query:
      """
      MATCH (:player{name:"Tony Parker"})-[r]->(m) where exists(m.age) return r
      """
    Then a SemanticError should be raised at runtime: To get the property of the vertex in `m.age', should use the format `var.tag.prop'
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)-[]-(v3)
      WITH v3.name as names
      RETURN count(names)
      """
    Then a SemanticError should be raised at runtime: To get the property of the vertex in `v3.name', should use the format `var.tag.prop'

  Scenario: filter is not a valid expression
    When executing query:
      """
      MATCH (v:player) WHERE v.player.name-'n'=="Tim Duncann" RETURN v
      """
    Then a SemanticError should be raised at runtime: `(v.player.name-"n")' is not a valid expression, can not apply `-' to `__EMPTY__' and `STRING'.
    When executing query:
      """
      MATCH (v:player) WHERE v.player.name+'n'=="Tim Duncann" RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                          |
      | ("Tim Duncan":bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

  Scenario: Sequential sentence
    When executing query:
      """
      USE nba;
      MATCH (n:player) WHERE id(n) == "Boris Diaw" RETURN n;
      """
    Then the result should be, in any order:
      | n                                                   |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"}) |

  Scenario: parse match node vs parenthesized expression
    When executing query:
      """
      MATCH (v) WHERE id(v) == 'Boris Diaw' RETURN (v)
      """
    Then the result should be, in any order:
      | v                                                   |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"}) |

  Scenario: Match with id when all tag is dropped, ent-#1420
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS player(name string, age int);
      CREATE TAG IF NOT EXISTS team(name string);
      CREATE TAG INDEX IF NOT EXISTS player_index_1 on player(name(10), age);
      """
    And wait 5 seconds
    When try to execute query:
      """
      INSERT VERTEX player() VALUES "v2":();
      INSERT VERTEX player(name, age) VALUES "v3":("v3", 18);
      UPSERT VERTEX ON player "v4" SET name = "v4", age = 18;
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH (v) WHERE id(v) in ["v1", "v2", "v3", "v4"] return id(v) limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | id(v) |
      | "v2"  |
      | "v3"  |
      | "v4"  |
    When try to execute query:
      """
      DROP TAG INDEX player_index_1;
      DROP TAG player;
      """
    Then the execution should be successful
    And wait 5 seconds
    When executing query:
      """
      MATCH (v) WHERE id(v) in ["v1", "v2", "v3", "v4"] return id(v) limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | id(v) |

  Scenario: match_with_wrong_syntax
    When executing query:
      """
      MATCH (v{name: "Tim Duncan"}) return v
      """
    Then a SemanticError should be raised at runtime: `name:"Tim Duncan"': No tag found for property.

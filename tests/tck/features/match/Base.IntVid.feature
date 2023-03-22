# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
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
      MATCH (:player{name:"Tony Parker"})-[r]->(m) where exists(m.team.likeness) return r, exists({a:12}.a)
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
      match (v:player{age: -5}) return v
      """
    Then the result should be, in any order, with relax comparison:
      | v |
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

  Scenario: Scan
    When executing query:
      """
      MATCH (v) return count(v)
      """
    Then the result should be, in any order:
      | count(v) |
      | 86       |
    When executing query:
      """
      MATCH (v:player:bachelor) RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                          |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
    When executing query:
      """
      MATCH (v:player{age:23}:bachelor) RETURN v
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      MATCH () -[]-> (v) return *
      """
    Then the result should be, in any order:
      | v                                                                                                          |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (-1782445125509592239:player{age:33,name:"LaMarcus Aldridge"})                                             |
      | (3394245602834314645:player{age:41,name:"Manu Ginobili"})                                                  |
      | (-4246510323023722591:player{age:31,name:"Danny Green"})                                                   |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (3394245602834314645:player{age:41,name:"Manu Ginobili"})                                                  |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (-7187791973189815797:player{age:29,name:"James Harden"})                                                  |
      | (-1808363682563220400:player{age:23,name:"Kristaps Porzingis"})                                            |
      | (8666973159269157201:player{age:40,name:"Dirk Nowitzki"})                                                  |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (-5725771886986242911:team{name:"Heat"})                                                                   |
      | (-24407060583402289:team{name:"Knicks"})                                                                   |
      | (6663720087669302163:player{age:45,name:"Steve Nash"})                                                     |
      | (6725794663354105876:team{name:"Timberwolves"})                                                            |
      | (-387811533900522498:team{name:"Jazz"})                                                                    |
      | (8319893554523355767:team{name:"Trail Blazers"})                                                           |
      | (-1782445125509592239:player{age:33,name:"LaMarcus Aldridge"})                                             |
      | (-7081823478649345536:team{name:"Rockets"})                                                                |
      | (-8700501352446189652:team{name:"Nuggets"})                                                                |
      | (-2708974440339147602:team{name:"Thunders"})                                                               |
      | (-24407060583402289:team{name:"Knicks"})                                                                   |
      | (-6952676908621237908:player{age:33,name:"Chris Paul"})                                                    |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-4725743394557506923:player{age:37,name:"Dwyane Wade"})                                                   |
      | (4349533032529872265:team{name:"76ers"})                                                                   |
      | (-7419439655175297510:player{age:25,name:"Joel Embiid"})                                                   |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (1106406639891543428:team{name:"Hornets"})                                                                 |
      | (-1782445125509592239:player{age:33,name:"LaMarcus Aldridge"})                                             |
      | (-8310021930715358072:player{age:25,name:"Kyle Anderson"})                                                 |
      | (3394245602834314645:player{age:41,name:"Manu Ginobili"})                                                  |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-1782445125509592239:player{age:33,name:"LaMarcus Aldridge"})                                             |
      | (3394245602834314645:player{age:41,name:"Manu Ginobili"})                                                  |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-3911019097593851745:team{name:"Warriors"})                                                               |
      | (2851446500980388888:team{name:"Pelicans"})                                                                |
      | (7276941027486377550:team{name:"Bulls"})                                                                   |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (2219497852896967815:team{name:"Kings"})                                                                   |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (7739801094663213767:team{name:"Celtics"})                                                                 |
      | (7749522836357656167:player{age:43,name:"Ray Allen"})                                                      |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-1309840256273367735:team{name:"Pacers"})                                                                 |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (1106406639891543428:team{name:"Hornets"})                                                                 |
      | (-3911019097593851745:team{name:"Warriors"})                                                               |
      | (3973677957398111287:team{name:"Hawks"})                                                                   |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (4349533032529872265:team{name:"76ers"})                                                                   |
      | (3394245602834314645:player{age:41,name:"Manu Ginobili"})                                                  |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-4249073228602996854:team{name:"Clippers"})                                                               |
      | (-2742277443392542725:team{name:"Pistons"})                                                                |
      | (-6952676908621237908:player{age:33,name:"Chris Paul"})                                                    |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (3973677957398111287:team{name:"Hawks"})                                                                   |
      | (8076177756672387643:team{name:"Grizzlies"})                                                               |
      | (2219497852896967815:team{name:"Kings"})                                                                   |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (635461671590608292:team{name:"Raptors"})                                                                  |
      | (-2318401216565722165:team{name:"Nets"})                                                                   |
      | (-9110170398241263635:team{name:"Magic"})                                                                  |
      | (4823234394086728974:player{age:39,name:"Tracy McGrady"})                                                  |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (8076177756672387643:team{name:"Grizzlies"})                                                               |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (-7187791973189815797:player{age:29,name:"James Harden"})                                                  |
      | (3778194419743477824:player{age:30,name:"Kevin Durant"})                                                   |
      | (-6952676908621237908:player{age:33,name:"Chris Paul"})                                                    |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-8310021930715358072:player{age:25,name:"Kyle Anderson"})                                                 |
      | (3394245602834314645:player{age:41,name:"Manu Ginobili"})                                                  |
      | (-4246510323023722591:player{age:31,name:"Danny Green"})                                                   |
      | (-2953535798644081749:player{age:30,name:"Russell Westbrook"})                                             |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (-8630442141511519924:team{name:"Cavaliers"})                                                              |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (-5725771886986242911:team{name:"Heat"})                                                                   |
      | (7739801094663213767:team{name:"Celtics"})                                                                 |
      | (-9110170398241263635:team{name:"Magic"})                                                                  |
      | (-6714656557196607176:player{age:31,name:"JaVale McGee"})                                                  |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (3973677957398111287:team{name:"Hawks"})                                                                   |
      | (7276941027486377550:team{name:"Bulls"})                                                                   |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (1106406639891543428:team{name:"Hornets"})                                                                 |
      | (2219497852896967815:team{name:"Kings"})                                                                   |
      | (4349533032529872265:team{name:"76ers"})                                                                   |
      | (-3911019097593851745:team{name:"Warriors"})                                                               |
      | (635461671590608292:team{name:"Raptors"})                                                                  |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (1106406639891543428:team{name:"Hornets"})                                                                 |
      | (-4246510323023722591:player{age:31,name:"Danny Green"})                                                   |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-7081823478649345536:team{name:"Rockets"})                                                                |
      | (1106406639891543428:team{name:"Hornets"})                                                                 |
      | (-4249073228602996854:team{name:"Clippers"})                                                               |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-4725743394557506923:player{age:37,name:"Dwyane Wade"})                                                   |
      | (-4722009539442865199:player{age:34,name:"Carmelo Anthony"})                                               |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (3973677957398111287:team{name:"Hawks"})                                                                   |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (1106406639891543428:team{name:"Hornets"})                                                                 |
      | (-387811533900522498:team{name:"Jazz"})                                                                    |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-2708974440339147602:team{name:"Thunders"})                                                               |
      | (-7187791973189815797:player{age:29,name:"James Harden"})                                                  |
      | (4312213929524069862:player{age:28,name:"Paul George"})                                                    |
      | (8076177756672387643:team{name:"Grizzlies"})                                                               |
      | (7276941027486377550:team{name:"Bulls"})                                                                   |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (5806152984018701956:team{name:"Bucks"})                                                                   |
      | (-2748242588091293411:player{age:34,name:"Marc Gasol"})                                                    |
      | (-2308681984240312228:player{age:40,name:"Kobe Bryant"})                                                   |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (8319893554523355767:team{name:"Trail Blazers"})                                                           |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (6663720087669302163:player{age:45,name:"Steve Nash"})                                                     |
      | (-4725743394557506923:player{age:37,name:"Dwyane Wade"})                                                   |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (-24407060583402289:team{name:"Knicks"})                                                                   |
      | (-1527627220316645914:player{age:20,name:"Luka Doncic"})                                                   |
      | (-3911019097593851745:team{name:"Warriors"})                                                               |
      | (8136836558163210487:player{age:31,name:"Stephen Curry"})                                                  |
      | (8076177756672387643:team{name:"Grizzlies"})                                                               |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (2219497852896967815:team{name:"Kings"})                                                                   |
      | (635461671590608292:team{name:"Raptors"})                                                                  |
      | (-1782445125509592239:player{age:33,name:"LaMarcus Aldridge"})                                             |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (-2318401216565722165:team{name:"Nets"})                                                                   |
      | (-24407060583402289:team{name:"Knicks"})                                                                   |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (8666973159269157201:player{age:40,name:"Dirk Nowitzki"})                                                  |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (6663720087669302163:player{age:45,name:"Steve Nash"})                                                     |
      | (-3911019097593851745:team{name:"Warriors"})                                                               |
      | (-2708974440339147602:team{name:"Thunders"})                                                               |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (-3911019097593851745:team{name:"Warriors"})                                                               |
      | (-8700501352446189652:team{name:"Nuggets"})                                                                |
      | (7810074228982596296:team{name:"Wizards"})                                                                 |
      | (-7081823478649345536:team{name:"Rockets"})                                                                |
      | (4823234394086728974:player{age:39,name:"Tracy McGrady"})                                                  |
      | (-2886538900410297738:player{age:47,name:"Shaquille O'Neal"})                                              |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (7739801094663213767:team{name:"Celtics"})                                                                 |
      | (-2742277443392542725:team{name:"Pistons"})                                                                |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-8630442141511519924:team{name:"Cavaliers"})                                                              |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (635461671590608292:team{name:"Raptors"})                                                                  |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-8630442141511519924:team{name:"Cavaliers"})                                                              |
      | (7276941027486377550:team{name:"Bulls"})                                                                   |
      | (-5725771886986242911:team{name:"Heat"})                                                                   |
      | (-5725771886986242911:team{name:"Heat"})                                                                   |
      | (-6952676908621237908:player{age:33,name:"Chris Paul"})                                                    |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-4722009539442865199:player{age:34,name:"Carmelo Anthony"})                                               |
      | (-1309840256273367735:team{name:"Pacers"})                                                                 |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (635461671590608292:team{name:"Raptors"})                                                                  |
      | (-7081823478649345536:team{name:"Rockets"})                                                                |
      | (3973677957398111287:team{name:"Hawks"})                                                                   |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (1106406639891543428:team{name:"Hornets"})                                                                 |
      | (7810074228982596296:team{name:"Wizards"})                                                                 |
      | (-9110170398241263635:team{name:"Magic"})                                                                  |
      | (4349533032529872265:team{name:"76ers"})                                                                   |
      | (-3310404127039111439:player{age:22,name:"Ben Simmons"})                                                   |
      | (5806152984018701956:team{name:"Bucks"})                                                                   |
      | (-5725771886986242911:team{name:"Heat"})                                                                   |
      | (-2708974440339147602:team{name:"Thunders"})                                                               |
      | (7739801094663213767:team{name:"Celtics"})                                                                 |
      | (-7984366606588005471:player{age:33,name:"Rajon Rondo"})                                                   |
      | (8076177756672387643:team{name:"Grizzlies"})                                                               |
      | (635461671590608292:team{name:"Raptors"})                                                                  |
      | (8988238998692066522:player{age:38,name:"Paul Gasol"})                                                     |
      | (5806152984018701956:team{name:"Bucks"})                                                                   |
      | (-1309840256273367735:team{name:"Pacers"})                                                                 |
      | (-2708974440339147602:team{name:"Thunders"})                                                               |
      | (-2953535798644081749:player{age:30,name:"Russell Westbrook"})                                             |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (4349533032529872265:team{name:"76ers"})                                                                   |
      | (-9110170398241263635:team{name:"Magic"})                                                                  |
      | (-4249073228602996854:team{name:"Clippers"})                                                               |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (-24407060583402289:team{name:"Knicks"})                                                                   |
      | (-7081823478649345536:team{name:"Rockets"})                                                                |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (635461671590608292:team{name:"Raptors"})                                                                  |
      | (-9110170398241263635:team{name:"Magic"})                                                                  |
      | (6293765385213992205:player{age:46,name:"Grant Hill"})                                                     |
      | (-2308681984240312228:player{age:40,name:"Kobe Bryant"})                                                   |
      | (-3212290852619976819:player{age:32,name:"Rudy Gay"})                                                      |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (-4249073228602996854:team{name:"Clippers"})                                                               |
      | (-9110170398241263635:team{name:"Magic"})                                                                  |
      | (-2742277443392542725:team{name:"Pistons"})                                                                |
      | (4823234394086728974:player{age:39,name:"Tracy McGrady"})                                                  |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (8666973159269157201:player{age:40,name:"Dirk Nowitzki"})                                                  |
      | (-8379929135833483044:player{age:36,name:"Amar'e Stoudemire"})                                             |
      | (8136836558163210487:player{age:31,name:"Stephen Curry"})                                                  |
      | (-8630442141511519924:team{name:"Cavaliers"})                                                              |
      | (7739801094663213767:team{name:"Celtics"})                                                                 |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-7081823478649345536:team{name:"Rockets"})                                                                |
      | (-2708974440339147602:team{name:"Thunders"})                                                               |
      | (-2953535798644081749:player{age:30,name:"Russell Westbrook"})                                             |
      | (-8630442141511519924:team{name:"Cavaliers"})                                                              |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (-5725771886986242911:team{name:"Heat"})                                                                   |
      | (-8630442141511519924:team{name:"Cavaliers"})                                                              |
      | (7749522836357656167:player{age:43,name:"Ray Allen"})                                                      |
    When executing query:
      """
      MATCH () --> (v) --> () return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 511      |
    # The 0 step means node scan in fact, but p and t has no label or properties for index seek
    # So it's not workable now
    When executing query:
      """
      MATCH (p)-[:serve*0..3]->(t) RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                          |
      | (-2742277443392542725:team{name:"Pistons"})                                                                |
      | (-387811533900522498:team{name:"Jazz"})                                                                    |
      | (-9110170398241263635:team{name:"Magic"})                                                                  |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-1527627220316645914:player{age:20,name:"Luka Doncic"})                                                   |
      | (-8379929135833483044:player{age:36,name:"Amar'e Stoudemire"})                                             |
      | (3514083604070023641:player{age:-4,name:NULL})                                                             |
      | (8939337962333576684:player{age:28,name:"Ricky Rubio"})                                                    |
      | (-8899043049306086446:player{age:28,name:"Damian Lillard"})                                                |
      | (-4722009539442865199:player{age:34,name:"Carmelo Anthony"})                                               |
      | (-24407060583402289:team{name:"Knicks"})                                                                   |
      | (-2318401216565722165:team{name:"Nets"})                                                                   |
      | (-3310404127039111439:player{age:22,name:"Ben Simmons"})                                                   |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (7739801094663213767:team{name:"Celtics"})                                                                 |
      | (-2708974440339147602:team{name:"Thunders"})                                                               |
      | (-8700501352446189652:team{name:"Nuggets"})                                                                |
      | (-5725771886986242911:team{name:"Heat"})                                                                   |
      | (8136836558163210487:player{age:31,name:"Stephen Curry"})                                                  |
      | (-7984366606588005471:player{age:33,name:"Rajon Rondo"})                                                   |
      | (3394245602834314645:player{age:41,name:"Manu Ginobili"})                                                  |
      | (2922505246153125262:player{age:38,name:"David West"})                                                     |
      | (7011978635221384332:team{name:"Mavericks"})                                                               |
      | (-8160811731890648949:player{age:34,name:"Tiago Splitter"})                                                |
      | (4349533032529872265:team{name:"76ers"})                                                                   |
      | (-6761803129056739448:player{age:30,name:"Blake Griffin"})                                                 |
      | (2219497852896967815:team{name:"Kings"})                                                                   |
      | (5806152984018701956:team{name:"Bucks"})                                                                   |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-8310021930715358072:player{age:25,name:"Kyle Anderson"})                                                 |
      | (5209979940224249985:player{age:29,name:"Dejounte Murray"})                                                |
      | (635461671590608292:team{name:"Raptors"})                                                                  |
      | (-4249073228602996854:team{name:"Clippers"})                                                               |
      | (8319893554523355767:team{name:"Trail Blazers"})                                                           |
      | (-2886538900410297738:player{age:47,name:"Shaquille O'Neal"})                                              |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-3911019097593851745:team{name:"Warriors"})                                                               |
      | (-6952676908621237908:player{age:33,name:"Chris Paul"})                                                    |
      | (7193291116733635180:team{name:"Spurs"})                                                                   |
      | (-7391649757245641883:player{age:36,name:"Boris Diaw"})                                                    |
      | (-2953535798644081749:player{age:30,name:"Russell Westbrook"})                                             |
      | (4064953130110781538:player{age:-3,name:NULL})                                                             |
      | (8988238998692066522:player{age:38,name:"Paul Gasol"})                                                     |
      | (7177621023936641989:player{age:-2,name:NULL})                                                             |
      | (4896863909272272220:team{name:"Lakers"})                                                                  |
      | (-2308681984240312228:player{age:40,name:"Kobe Bryant"})                                                   |
      | (-1782445125509592239:player{age:33,name:"LaMarcus Aldridge"})                                             |
      | (8666973159269157201:player{age:40,name:"Dirk Nowitzki"})                                                  |
      | (-1808363682563220400:player{age:23,name:"Kristaps Porzingis"})                                            |
      | (-3159397121379009673:player{age:29,name:"Klay Thompson"})                                                 |
      | (-8630442141511519924:team{name:"Cavaliers"})                                                              |
      | (-1309840256273367735:team{name:"Pacers"})                                                                 |
      | (7810074228982596296:team{name:"Wizards"})                                                                 |
      | (-3212290852619976819:player{age:32,name:"Rudy Gay"})                                                      |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (3778194419743477824:player{age:30,name:"Kevin Durant"})                                                   |
      | (-6714656557196607176:player{age:31,name:"JaVale McGee"})                                                  |
      | (3973677957398111287:team{name:"Hawks"})                                                                   |
      | (4651420795228053868:player{age:38,name:"Yao Ming"})                                                       |
      | (-7034133662712739796:player{age:32,name:"Aron Baynes"})                                                   |
      | (-4246510323023722591:player{age:31,name:"Danny Green"})                                                   |
      | (-4725743394557506923:player{age:37,name:"Dwyane Wade"})                                                   |
      | (-2020854379915135447:player{age:27,name:"Cory Joseph"})                                                   |
      | (-7291604415594599833:player{age:33,name:"Dwight Howard"})                                                 |
      | (-7419439655175297510:player{age:25,name:"Joel Embiid"})                                                   |
      | (7749522836357656167:player{age:43,name:"Ray Allen"})                                                      |
      | (-2748242588091293411:player{age:34,name:"Marc Gasol"})                                                    |
      | (2851446500980388888:team{name:"Pelicans"})                                                                |
      | (-2500659820593255893:player{age:24,name:"Giannis Antetokounmpo"})                                         |
      | (6725794663354105876:team{name:"Timberwolves"})                                                            |
      | (4312213929524069862:player{age:28,name:"Paul George"})                                                    |
      | (6315667670552355223:player{age:29,name:"Jonathon Simmons"})                                               |
      | (-835937448829988801:player{age:0,name:"Nobody"})                                                          |
      | (2357802964435032110:player{age:30,name:"DeAndre Jordan"})                                                 |
      | (868103967282670864:team{name:"Suns"})                                                                     |
      | (1106406639891543428:team{name:"Hornets"})                                                                 |
      | (4823234394086728974:player{age:39,name:"Tracy McGrady"})                                                  |
      | (8076177756672387643:team{name:"Grizzlies"})                                                               |
      | (6293765385213992205:player{age:46,name:"Grant Hill"})                                                     |
      | (6663720087669302163:player{age:45,name:"Steve Nash"})                                                     |
      | (-7276938555819111674:player{age:26,name:"Kyrie Irving"})                                                  |
      | (-7187791973189815797:player{age:29,name:"James Harden"})                                                  |
      | (-4907338031678696446:player{age:-1,name:NULL})                                                            |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (7276941027486377550:team{name:"Bulls"})                                                                   |
      | (-7081823478649345536:team{name:"Rockets"})                                                                |
      | (5662213458193308137:player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | (-1527627220316645914:player{age:20,name:"Luka Doncic"})                                                   |
      | (-8379929135833483044:player{age:36,name:"Amar'e Stoudemire"})                                             |
      | (-8379929135833483044:player{age:36,name:"Amar'e Stoudemire"})                                             |
      | (-8379929135833483044:player{age:36,name:"Amar'e Stoudemire"})                                             |
      | (8939337962333576684:player{age:28,name:"Ricky Rubio"})                                                    |
      | (8939337962333576684:player{age:28,name:"Ricky Rubio"})                                                    |
      | (-8899043049306086446:player{age:28,name:"Damian Lillard"})                                                |
      | (-4722009539442865199:player{age:34,name:"Carmelo Anthony"})                                               |
      | (-4722009539442865199:player{age:34,name:"Carmelo Anthony"})                                               |
      | (-4722009539442865199:player{age:34,name:"Carmelo Anthony"})                                               |
      | (-4722009539442865199:player{age:34,name:"Carmelo Anthony"})                                               |
      | (-3310404127039111439:player{age:22,name:"Ben Simmons"})                                                   |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (-7579316172763586624:player{age:36,name:"Tony Parker"})                                                   |
      | (8136836558163210487:player{age:31,name:"Stephen Curry"})                                                  |
      | (-7984366606588005471:player{age:33,name:"Rajon Rondo"})                                                   |
      | (-7984366606588005471:player{age:33,name:"Rajon Rondo"})                                                   |
      | (-7984366606588005471:player{age:33,name:"Rajon Rondo"})                                                   |
      | (-7984366606588005471:player{age:33,name:"Rajon Rondo"})                                                   |
      | (-7984366606588005471:player{age:33,name:"Rajon Rondo"})                                                   |
      | (-7984366606588005471:player{age:33,name:"Rajon Rondo"})                                                   |
      | (3394245602834314645:player{age:41,name:"Manu Ginobili"})                                                  |
      | (2922505246153125262:player{age:38,name:"David West"})                                                     |
      | (2922505246153125262:player{age:38,name:"David West"})                                                     |
      | (2922505246153125262:player{age:38,name:"David West"})                                                     |
      | (2922505246153125262:player{age:38,name:"David West"})                                                     |
      | (-8160811731890648949:player{age:34,name:"Tiago Splitter"})                                                |
      | (-8160811731890648949:player{age:34,name:"Tiago Splitter"})                                                |
      | (-8160811731890648949:player{age:34,name:"Tiago Splitter"})                                                |
      | (-6761803129056739448:player{age:30,name:"Blake Griffin"})                                                 |
      | (-6761803129056739448:player{age:30,name:"Blake Griffin"})                                                 |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-6581004839648359804:player{age:42,name:"Vince Carter"})                                                  |
      | (-8310021930715358072:player{age:25,name:"Kyle Anderson"})                                                 |
      | (-8310021930715358072:player{age:25,name:"Kyle Anderson"})                                                 |
      | (5209979940224249985:player{age:29,name:"Dejounte Murray"})                                                |
      | (-2886538900410297738:player{age:47,name:"Shaquille O'Neal"})                                              |
      | (-2886538900410297738:player{age:47,name:"Shaquille O'Neal"})                                              |
      | (-2886538900410297738:player{age:47,name:"Shaquille O'Neal"})                                              |
      | (-2886538900410297738:player{age:47,name:"Shaquille O'Neal"})                                              |
      | (-2886538900410297738:player{age:47,name:"Shaquille O'Neal"})                                              |
      | (-2886538900410297738:player{age:47,name:"Shaquille O'Neal"})                                              |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-8206304902611825447:player{age:32,name:"Marco Belinelli"})                                               |
      | (-6952676908621237908:player{age:33,name:"Chris Paul"})                                                    |
      | (-6952676908621237908:player{age:33,name:"Chris Paul"})                                                    |
      | (-6952676908621237908:player{age:33,name:"Chris Paul"})                                                    |
      | (-7391649757245641883:player{age:36,name:"Boris Diaw"})                                                    |
      | (-7391649757245641883:player{age:36,name:"Boris Diaw"})                                                    |
      | (-7391649757245641883:player{age:36,name:"Boris Diaw"})                                                    |
      | (-7391649757245641883:player{age:36,name:"Boris Diaw"})                                                    |
      | (-7391649757245641883:player{age:36,name:"Boris Diaw"})                                                    |
      | (-2953535798644081749:player{age:30,name:"Russell Westbrook"})                                             |
      | (8988238998692066522:player{age:38,name:"Paul Gasol"})                                                     |
      | (8988238998692066522:player{age:38,name:"Paul Gasol"})                                                     |
      | (8988238998692066522:player{age:38,name:"Paul Gasol"})                                                     |
      | (8988238998692066522:player{age:38,name:"Paul Gasol"})                                                     |
      | (8988238998692066522:player{age:38,name:"Paul Gasol"})                                                     |
      | (-2308681984240312228:player{age:40,name:"Kobe Bryant"})                                                   |
      | (-1782445125509592239:player{age:33,name:"LaMarcus Aldridge"})                                             |
      | (-1782445125509592239:player{age:33,name:"LaMarcus Aldridge"})                                             |
      | (8666973159269157201:player{age:40,name:"Dirk Nowitzki"})                                                  |
      | (-1808363682563220400:player{age:23,name:"Kristaps Porzingis"})                                            |
      | (-1808363682563220400:player{age:23,name:"Kristaps Porzingis"})                                            |
      | (-3159397121379009673:player{age:29,name:"Klay Thompson"})                                                 |
      | (-3212290852619976819:player{age:32,name:"Rudy Gay"})                                                      |
      | (-3212290852619976819:player{age:32,name:"Rudy Gay"})                                                      |
      | (-3212290852619976819:player{age:32,name:"Rudy Gay"})                                                      |
      | (-3212290852619976819:player{age:32,name:"Rudy Gay"})                                                      |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (7339407009759737412:player{age:45,name:"Jason Kidd"})                                                     |
      | (3778194419743477824:player{age:30,name:"Kevin Durant"})                                                   |
      | (3778194419743477824:player{age:30,name:"Kevin Durant"})                                                   |
      | (-6714656557196607176:player{age:31,name:"JaVale McGee"})                                                  |
      | (-6714656557196607176:player{age:31,name:"JaVale McGee"})                                                  |
      | (-6714656557196607176:player{age:31,name:"JaVale McGee"})                                                  |
      | (-6714656557196607176:player{age:31,name:"JaVale McGee"})                                                  |
      | (-6714656557196607176:player{age:31,name:"JaVale McGee"})                                                  |
      | (4651420795228053868:player{age:38,name:"Yao Ming"})                                                       |
      | (-7034133662712739796:player{age:32,name:"Aron Baynes"})                                                   |
      | (-7034133662712739796:player{age:32,name:"Aron Baynes"})                                                   |
      | (-7034133662712739796:player{age:32,name:"Aron Baynes"})                                                   |
      | (-4246510323023722591:player{age:31,name:"Danny Green"})                                                   |
      | (-4246510323023722591:player{age:31,name:"Danny Green"})                                                   |
      | (-4246510323023722591:player{age:31,name:"Danny Green"})                                                   |
      | (-4725743394557506923:player{age:37,name:"Dwyane Wade"})                                                   |
      | (-4725743394557506923:player{age:37,name:"Dwyane Wade"})                                                   |
      | (-4725743394557506923:player{age:37,name:"Dwyane Wade"})                                                   |
      | (-4725743394557506923:player{age:37,name:"Dwyane Wade"})                                                   |
      | (-2020854379915135447:player{age:27,name:"Cory Joseph"})                                                   |
      | (-2020854379915135447:player{age:27,name:"Cory Joseph"})                                                   |
      | (-2020854379915135447:player{age:27,name:"Cory Joseph"})                                                   |
      | (-7291604415594599833:player{age:33,name:"Dwight Howard"})                                                 |
      | (-7291604415594599833:player{age:33,name:"Dwight Howard"})                                                 |
      | (-7291604415594599833:player{age:33,name:"Dwight Howard"})                                                 |
      | (-7291604415594599833:player{age:33,name:"Dwight Howard"})                                                 |
      | (-7291604415594599833:player{age:33,name:"Dwight Howard"})                                                 |
      | (-7291604415594599833:player{age:33,name:"Dwight Howard"})                                                 |
      | (-7419439655175297510:player{age:25,name:"Joel Embiid"})                                                   |
      | (7749522836357656167:player{age:43,name:"Ray Allen"})                                                      |
      | (7749522836357656167:player{age:43,name:"Ray Allen"})                                                      |
      | (7749522836357656167:player{age:43,name:"Ray Allen"})                                                      |
      | (7749522836357656167:player{age:43,name:"Ray Allen"})                                                      |
      | (-2748242588091293411:player{age:34,name:"Marc Gasol"})                                                    |
      | (-2748242588091293411:player{age:34,name:"Marc Gasol"})                                                    |
      | (-2500659820593255893:player{age:24,name:"Giannis Antetokounmpo"})                                         |
      | (4312213929524069862:player{age:28,name:"Paul George"})                                                    |
      | (4312213929524069862:player{age:28,name:"Paul George"})                                                    |
      | (6315667670552355223:player{age:29,name:"Jonathon Simmons"})                                               |
      | (6315667670552355223:player{age:29,name:"Jonathon Simmons"})                                               |
      | (6315667670552355223:player{age:29,name:"Jonathon Simmons"})                                               |
      | (2357802964435032110:player{age:30,name:"DeAndre Jordan"})                                                 |
      | (2357802964435032110:player{age:30,name:"DeAndre Jordan"})                                                 |
      | (2357802964435032110:player{age:30,name:"DeAndre Jordan"})                                                 |
      | (4823234394086728974:player{age:39,name:"Tracy McGrady"})                                                  |
      | (4823234394086728974:player{age:39,name:"Tracy McGrady"})                                                  |
      | (4823234394086728974:player{age:39,name:"Tracy McGrady"})                                                  |
      | (4823234394086728974:player{age:39,name:"Tracy McGrady"})                                                  |
      | (6293765385213992205:player{age:46,name:"Grant Hill"})                                                     |
      | (6293765385213992205:player{age:46,name:"Grant Hill"})                                                     |
      | (6293765385213992205:player{age:46,name:"Grant Hill"})                                                     |
      | (6293765385213992205:player{age:46,name:"Grant Hill"})                                                     |
      | (6663720087669302163:player{age:45,name:"Steve Nash"})                                                     |
      | (6663720087669302163:player{age:45,name:"Steve Nash"})                                                     |
      | (6663720087669302163:player{age:45,name:"Steve Nash"})                                                     |
      | (6663720087669302163:player{age:45,name:"Steve Nash"})                                                     |
      | (-7276938555819111674:player{age:26,name:"Kyrie Irving"})                                                  |
      | (-7276938555819111674:player{age:26,name:"Kyrie Irving"})                                                  |
      | (-7187791973189815797:player{age:29,name:"James Harden"})                                                  |
      | (-7187791973189815797:player{age:29,name:"James Harden"})                                                  |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |
      | (-8864869605086585744:player{age:34,name:"LeBron James"})                                                  |

  Scenario: Get property or tag from a vertex
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"}) RETURN v.name AS vname
      """
    Then the result should be, in any order:
      | vname |
      | NULL  |
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"}) RETURN v.player AS vtag
      """
    Then the result should be, in any order:
      | vtag                |
      | {name:"Tim Duncan"} |
    When executing query:
      """
      MATCH (v:player)-[]->(b) WHERE v.age > 30 RETURN v.player.name AS vname
      """
    Then the result should be, in any order:
      | vname |
    When executing query:
      """
      MATCH (v:player)-[:like]->(b) WHERE v.player.age > 30 WITH v.player.name AS vname, b.age AS bage RETURN vname, bage
      """
    Then the result should be, in any order:
      | vname               | bage |
      | "Rajon Rondo"       | NULL |
      | "Manu Ginobili"     | NULL |
      | "Tracy McGrady"     | NULL |
      | "Tracy McGrady"     | NULL |
      | "Tracy McGrady"     | NULL |
      | "Tim Duncan"        | NULL |
      | "Tim Duncan"        | NULL |
      | "Chris Paul"        | NULL |
      | "Chris Paul"        | NULL |
      | "Chris Paul"        | NULL |
      | "Rudy Gay"          | NULL |
      | "Paul Gasol"        | NULL |
      | "Paul Gasol"        | NULL |
      | "Boris Diaw"        | NULL |
      | "Boris Diaw"        | NULL |
      | "Aron Baynes"       | NULL |
      | "Carmelo Anthony"   | NULL |
      | "Carmelo Anthony"   | NULL |
      | "Carmelo Anthony"   | NULL |
      | "Danny Green"       | NULL |
      | "Danny Green"       | NULL |
      | "Danny Green"       | NULL |
      | "Vince Carter"      | NULL |
      | "Vince Carter"      | NULL |
      | "Jason Kidd"        | NULL |
      | "Jason Kidd"        | NULL |
      | "Jason Kidd"        | NULL |
      | "Marc Gasol"        | NULL |
      | "Grant Hill"        | NULL |
      | "Steve Nash"        | NULL |
      | "Steve Nash"        | NULL |
      | "Steve Nash"        | NULL |
      | "Steve Nash"        | NULL |
      | "Tony Parker"       | NULL |
      | "Tony Parker"       | NULL |
      | "Tony Parker"       | NULL |
      | "Marco Belinelli"   | NULL |
      | "Marco Belinelli"   | NULL |
      | "Marco Belinelli"   | NULL |
      | "Yao Ming"          | NULL |
      | "Yao Ming"          | NULL |
      | "Dirk Nowitzki"     | NULL |
      | "Dirk Nowitzki"     | NULL |
      | "Dirk Nowitzki"     | NULL |
      | "Shaquille O'Neal"  | NULL |
      | "Shaquille O'Neal"  | NULL |
      | "LaMarcus Aldridge" | NULL |
      | "LaMarcus Aldridge" | NULL |
      | "Tiago Splitter"    | NULL |
      | "Tiago Splitter"    | NULL |
      | "Ray Allen"         | NULL |
      | "LeBron James"      | NULL |
      | "Amar'e Stoudemire" | NULL |
      | "Dwyane Wade"       | NULL |
      | "Dwyane Wade"       | NULL |
      | "Dwyane Wade"       | NULL |
    When executing query:
      """
      MATCH (:player{name:"Tony Parker"})-[r]->(m) where exists(m.age) RETURN r
      """
    Then the result should be, in any order:
      | r |
    When executing query:
      """
      MATCH (:player{name:"Tony Parker"})-[r]->(m) where exists(m.player.age) RETURN r
      """
    Then the result should be, in any order, with relax comparison:
      | r                                                                                    |
      | [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]     |
      | [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}] |
      | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]     |
      | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]        |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                                |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]                         |
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)-[]-(v3)
      WITH v3.name as names
      RETURN count(names) AS c
      """
    Then the result should be, in any order:
      | c |
      | 0 |
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-[]-(v2)-[]-(v3)
      WITH v3.player.name as names
      RETURN count(distinct names) AS c
      """
    Then the result should be, in any order:
      | c  |
      | 25 |

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

  Scenario: parse match node vs parenthesized expression
    When executing query:
      """
      MATCH (v) WHERE id(v) == hash('Boris Diaw') RETURN (v)
      """
    Then the result should be, in any order:
      | v                                                   |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"}) |

  Scenario: match with tag filter
    When executing query:
      """
      MATCH (a:team)-[e*0..1]-(b) where id(a) == hash('Tim Duncan') return b
      """
    Then the result should be, in any order, with relax comparison:
      | b |
    When executing query:
      """
      MATCH (a:team)-[e*0..0]-(b) where id(a) in [hash('Tim Duncan'), hash('Spurs')] return b
      """
    Then the result should be, in any order, with relax comparison:
      | b         |
      | ('Spurs') |
    When executing query:
      """
      MATCH (a:team)-[e*0..1]-(b) where id(a) in [hash('Tim Duncan'), hash('Spurs')] return b
      """
    Then the result should be, in any order, with relax comparison:
      | b                     |
      | ("Spurs")             |
      | ("Aron Baynes")       |
      | ("Boris Diaw")        |
      | ("Cory Joseph")       |
      | ("Danny Green")       |
      | ("David West")        |
      | ("Dejounte Murray")   |
      | ("Jonathon Simmons")  |
      | ("Kyle Anderson")     |
      | ("LaMarcus Aldridge") |
      | ("Manu Ginobili")     |
      | ("Marco Belinelli")   |
      | ("Paul Gasol")        |
      | ("Rudy Gay")          |
      | ("Tiago Splitter")    |
      | ("Tim Duncan")        |
      | ("Tony Parker")       |
      | ("Tracy McGrady")     |
      | ("Marco Belinelli")   |

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
    When profiling query:
      """
      MATCH (v:player) RETURN v.player.age LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | v.player.age |
      | /\d+/        |
      | /\d+/        |
      | /\d+/        |
    Then the execution plan should be:
      | id | name           | dependencies | operator info                 |
      | 5  | Project        | 4            |                               |
      | 4  | Limit          | 3            | {"offset": "0", "count": "3"} |
      | 3  | AppendVertices | 2            |                               |
      | 2  | IndexScan      | 1            | {"limit": "3"}                |
      | 1  | Start          |              |                               |
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
      MATCH (v:player {age: 29}) RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    When executing query:
      """
      MATCH (v:player {age: 29}) WHERE v.player.name STARTS WITH "J" RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
    When executing query:
      """
      MATCH (v:player) WHERE v.player.age >= 38 AND v.player.age < 45 RETURN v.player.name AS Name, v.player.age AS Age
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
      MATCH (v:player) where v.player.age > 9223372036854775807+1  RETURN v
      """
    Then a SemanticError should be raised at runtime: result of (9223372036854775807+1) cannot be represented as an integer
    When executing query:
      """
      MATCH (v:player) where v.player.age > -9223372036854775808-1  RETURN v
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
      MATCH (v1) -[:like]-> (v2:player{name: "Danny Green"})
      RETURN v1.player.name AS Name, v2.player.name AS Friend
      """
    Then the result should be, in any order:
      | Name              | Friend        |
      | "Dejounte Murray" | "Danny Green" |
      | "Marco Belinelli" | "Danny Green" |
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
      MATCH (v:player)-[e:like]-(v2)
      WHERE v.player.age == 38
      RETURN *
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
      MATCH (v:player)-[e:like]->(v2)
      WHERE v2.player.age > 45
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                         | e                                                        | v2                                                              |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})           | [:like "Yao Ming"->"Shaquille O'Neal" @0 {likeness: 90}] | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"}) |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"}) | [:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}]  | ("Grant Hill" :player{age: 46, name: "Grant Hill"})             |
    When executing query:
      """
      MATCH (v:player)-[e:like]->(v2)
      WHERE v.player.age == 38 and (v2.player.age < 35 or v2.player.age == 40)
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                   | e                                                     | v2                                                    |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"}) | [:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}] | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"}) |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"}) | [:like "Paul Gasol"->"Marc Gasol" @0 {likeness: 99}]  | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"})   |
    When executing query:
      """
      MATCH (v:player)-[e:like]->(v2)
      WHERE v.player.age == 38 and (v2.player.age < 35 or v2.player.age == 40) and e.likeness > 90
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                   | e                                                    | v2                                                  |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"}) | [:like "Paul Gasol"->"Marc Gasol" @0 {likeness: 99}] | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"}) |
    When executing query:
      """
      MATCH (v:player)-[e:like]->(v2)
      WHERE id(v) == "Tim Duncan"
      RETURN DISTINCT properties(e) as props, e
      """
    Then the result should be, in any order, with relax comparison:
      | props          | e                                                       |
      | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    When executing query:
      """
      MATCH (v:player)-[e:like]->(v2)
      WHERE id(v) == "Tim Duncan"
      RETURN DISTINCT properties(e) as props
      """
    Then the result should be, in any order, with relax comparison:
      | props          |
      | {likeness: 95} |
    When executing query:
      """
      MATCH (v1:player)-[e]->(v2)
      WHERE v2.player.age == 38 or (v2.team.name == 'Rockets' and v1.player.age == 38)
      RETURN
        v1.player.name AS Name,
        type(e) as Type,
        CASE WHEN v2.player.name IS NOT NULL THEN v2.player.name ELSE v2.team.name END AS FriendOrTeam
      """
    Then the result should be, in any order, with relax comparison:
      | Name         | Type    | FriendOrTeam |
      | "Yao Ming"   | "serve" | "Rockets"    |
      | "Marc Gasol" | "like"  | "Paul Gasol" |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) -[:like]- (v2)
      WHERE v1.player.age > 1000
      RETURN v1.player.name AS Name, v2.player.name AS Friend
      """
    Then the result should be, in any order:
      | Name | Friend |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) -[:like]- (v2:player{name: "Yao Ming"})
      RETURN v1.player.name AS Name, v2.player.name AS Friend
      """
    Then the result should be, in any order:
      | Name | Friend |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) -[e1:like]- (v2)
      WHERE e1.likeness_not_exists > 0
      RETURN v1.player.name AS Name, v2.player.name AS Friend
      """
    Then the result should be, in any order:
      | Name | Friend |
    When try to execute query:
      """
      MATCH (v1:player{name: "Danny Green"}) -[:like_not_exists]- (v2)
      RETURN v1.player.name AS Name, v2.player.name AS Friend
      """
    Then a SemanticError should be raised at runtime: `like_not_exists': Unknown edge type

  @hello
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
      MATCH (v1:player) -[:like]-> (v2) -[:like]-> (v3)
      WHERE v1.player.age == 28
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF, v3.player.name_not_exists AS NotExists
      """
    Then the result should be, in any order:
      | Player           | Friend              | FoF            | NotExists |
      | "Damian Lillard" | "LaMarcus Aldridge" | "Tim Duncan"   | NULL      |
      | "Damian Lillard" | "LaMarcus Aldridge" | "Tony Parker"  | NULL      |
      | "Paul George"    | "Russell Westbrook" | "James Harden" | NULL      |
      | "Paul George"    | "Russell Westbrook" | "Paul George"  | NULL      |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v2:player{age: 28}) -[:like]-> (v3)
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then the result should be, in any order:
      | Player              | Friend        | FoF                 |
      | "Russell Westbrook" | "Paul George" | "Russell Westbrook" |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v2) -[:like]-> (v3)
      WHERE v2.player.age == 28
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then the result should be, in any order:
      | Player              | Friend        | FoF                 |
      | "Russell Westbrook" | "Paul George" | "Russell Westbrook" |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v2) -[:like]-> (v3:player{age: 28})
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then the result should be, in any order:
      | Player            | Friend              | FoF           |
      | "Dejounte Murray" | "Russell Westbrook" | "Paul George" |
      | "James Harden"    | "Russell Westbrook" | "Paul George" |
      | "Paul George"     | "Russell Westbrook" | "Paul George" |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v2) -[:like]-> (v3)
      WHERE v3.player.age == 28
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then the result should be, in any order:
      | Player            | Friend              | FoF           |
      | "Dejounte Murray" | "Russell Westbrook" | "Paul George" |
      | "James Harden"    | "Russell Westbrook" | "Paul George" |
      | "Paul George"     | "Russell Westbrook" | "Paul George" |
    When executing query:
      """
      MATCH (v1) -[e1:like]-> (v2) -[e2:like]-> (v3)
      WHERE v1.player.age > 28 and e1.likeness > 90 and v2.player.age > 40 and e2.likeness > 90 and v3.player.age > 40
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then the result should be, in any order:
      | Player            | Friend       | FoF             |
      | "Dejounte Murray" | "Tim Duncan" | "Manu Ginobili" |
      | "Tony Parker"     | "Tim Duncan" | "Manu Ginobili" |
    When executing query:
      """
      MATCH (v1) -[e1:like]-> (v2) -[e2]-> (v3)
      WHERE v3.player.age == 38 or (v3.team.name == 'Rockets' and v1.player.age == 38)
      RETURN v1.player.name AS Player, v2.player.name AS Friend, type(e2) AS TYPE, v3.player.name AS FoF, v3.team.name AS FoT
      """
    Then the result should be, in any order:
      | Player       | Friend          | TYPE    | FoF          | FoT       |
      | "Paul Gasol" | "Marc Gasol"    | "like"  | "Paul Gasol" | NULL      |
      | "Yao Ming"   | "Tracy McGrady" | "serve" | NULL         | "Rockets" |
    When executing query:
      """
      MATCH (v1) -[e1:like]-> (v2) -[e2]-> (v3)
      WHERE v1.player.age > 1000
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then the result should be, in any order:
      | Player | Friend | FoF |
    When executing query:
      """
      MATCH (v1:player{name: "Danny Green"}) -[:like]-> (v2) -[:like]-> (v3:player{name: "Yao Ming"})
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then the result should be, in any order:
      | Player | Friend | FoF |
    When executing query:
      """
      MATCH (v1) -[e1:like]-> (v2) -[e2]-> (v3)
      WHERE e1.likeness_not_exists > 0
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then the result should be, in any order:
      | Player | Friend | FoF |
    When try to execute query:
      """
      MATCH (v1) -[e1:like]-> (v2) -[e2:like_not_exists]-> (v3)
      RETURN v1.player.name AS Player, v2.player.name AS Friend, v3.player.name AS FoF
      """
    Then a SemanticError should be raised at runtime: `like_not_exists': Unknown edge type
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
      MATCH p = (n:player{name:"Tony Parker"}) RETURN p,n
      """
    Then the result should be, in any order, with relax comparison:
      | p                 | n               |
      | <("Tony Parker")> | ("Tony Parker") |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m) RETURN p, n.player.name, m.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                           | n.player.name  | m.player.name |
      | <("LeBron James")-[:like@0]->("Ray Allen")> | "LeBron James" | "Ray Allen"   |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m) RETURN p, n.player.name, m.player.name
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
      MATCH p = (n:player{name:"LeBron James"})-[:like]-(m) RETURN p, n.player.name, m.player.name
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
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)-[:like]->(k) RETURN p, n.player.name, m.player.name, k.player.name
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
      | partition_num  | 1                |
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

  Scenario: Combination of some cypher clauses
    When executing query:
      """
      UNWIND ["Tony Parker", "Tim Duncan", "Yao Ming"] AS a MATCH (v:player) WHERE v.player.name == a RETURN distinct a, v
      """
    Then the result should be, in any order, with relax comparison:
      | a             | v                                                                                                           |
      | "Yao Ming"    | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             |
      | "Tim Duncan"  | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) |
      | "Tony Parker" | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
    When executing query:
      """
      WITH "Tony Parker" AS a MATCH (v:player) WHERE v.player.name == a RETURN a, v
      """
    Then the result should be, in any order, with relax comparison:
      | a             | v                                                     |
      | "Tony Parker" | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |

  Scenario: exists
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->() where exists(r.likeness) RETURN r, exists({a:12}.a)
      """
    Then the result should be, in any order, with relax comparison:
      | r                                                            | exists({a:12}.a) |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] | true             |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     | true             |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        | true             |
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->(m) where exists(m.player.likeness) RETURN r, exists({a:12}.a)
      """
    Then the result should be, in any order, with relax comparison:
      | r | exists({a:12}.a) |
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->(m) where exists({abc:123}.abc) RETURN r
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
      match (:player{name:"Tony Parker"})-[r]->(m) where exists({abc:123}.ab) RETURN r
      """
    Then the result should be, in any order, with relax comparison:
      | r |
    When executing query:
      """
      match (:player{name:"Tony Parker"})-[r]->(m) where exists(m.player.age) RETURN r
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
      match (v:player{age: -1}) RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                      |
      | ("Null1" :player{age: -1, name: NULL}) |
    When executing query:
      """
      match (v:player{age: +20}) RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                     |
      | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"}) |
    When executing query:
      """
      match (v:player{age: 1+19}) RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                     |
      | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"}) |
    When executing query:
      """
      match (v:player)-[e:like{likeness:-1}]->()  RETURN e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                       |
      | [:like "Blake Griffin"->"Chris Paul" @0 {likeness: -1}] |
      | [:like "Rajon Rondo"->"Ray Allen" @0 {likeness: -1}]    |
    When executing query:
      """
      match (v:player)-[e:like{likeness:40+50+5}]->()  RETURN e
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
      match (v:player)-[e:like{likeness:4*20+5}]->()  RETURN e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                    |
      | [:like "Jason Kidd"->"Dirk Nowitzki"@0{likeness:85}] |
      | [:like "Steve Nash"->"Jason Kidd"@0{likeness:85}]    |
    When executing query:
      """
      match (v:player)-[e:like{likeness:"99"}]->()  RETURN e
      """
    Then the result should be, in any order, with relax comparison:
      | e |
    When executing query:
      """
      match (v:player{age:"24"-1})  RETURN v
      """
    Then a SemanticError should be raised at runtime: Type error `("24"-1)'

  Scenario: No RETURN
    When executing query:
      """
      MATCH (v:player{name:"abc"})
      """
    Then a SyntaxError should be raised at runtime: syntax error near `)'
    When executing query:
      """
      MATCH (v:player) where v.player.name RETURN v
      """
    Then a ExecutionError should be raised at runtime: Failed to evaluate condition: v.player.name. For boolean conditions, please write in their full forms like <condition> == <true/false> or <condition> IS [NOT] NULL.

  Scenario: Scan
    When executing query:
      """
      MATCH (v) RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                   |
      | ("76ers":team{name:"76ers"})                                                                        |
      | ("Amar'e Stoudemire":player{age:36,name:"Amar'e Stoudemire"})                                       |
      | ("Aron Baynes":player{age:32,name:"Aron Baynes"})                                                   |
      | ("Ben Simmons":player{age:22,name:"Ben Simmons"})                                                   |
      | ("Blake Griffin":player{age:30,name:"Blake Griffin"})                                               |
      | ("Boris Diaw":player{age:36,name:"Boris Diaw"})                                                     |
      | ("Bucks":team{name:"Bucks"})                                                                        |
      | ("Bulls":team{name:"Bulls"})                                                                        |
      | ("Carmelo Anthony":player{age:34,name:"Carmelo Anthony"})                                           |
      | ("Cavaliers":team{name:"Cavaliers"})                                                                |
      | ("Celtics":team{name:"Celtics"})                                                                    |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("Clippers":team{name:"Clippers"})                                                                  |
      | ("Cory Joseph":player{age:27,name:"Cory Joseph"})                                                   |
      | ("Damian Lillard":player{age:28,name:"Damian Lillard"})                                             |
      | ("Danny Green":player{age:31,name:"Danny Green"})                                                   |
      | ("David West":player{age:38,name:"David West"})                                                     |
      | ("DeAndre Jordan":player{age:30,name:"DeAndre Jordan"})                                             |
      | ("Dejounte Murray":player{age:29,name:"Dejounte Murray"})                                           |
      | ("Dirk Nowitzki":player{age:40,name:"Dirk Nowitzki"})                                               |
      | ("Dwight Howard":player{age:33,name:"Dwight Howard"})                                               |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("Giannis Antetokounmpo":player{age:24,name:"Giannis Antetokounmpo"})                               |
      | ("Grant Hill":player{age:46,name:"Grant Hill"})                                                     |
      | ("Grizzlies":team{name:"Grizzlies"})                                                                |
      | ("Hawks":team{name:"Hawks"})                                                                        |
      | ("Heat":team{name:"Heat"})                                                                          |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("JaVale McGee":player{age:31,name:"JaVale McGee"})                                                 |
      | ("James Harden":player{age:29,name:"James Harden"})                                                 |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Jazz":team{name:"Jazz"})                                                                          |
      | ("Joel Embiid":player{age:25,name:"Joel Embiid"})                                                   |
      | ("Jonathon Simmons":player{age:29,name:"Jonathon Simmons"})                                         |
      | ("Kevin Durant":player{age:30,name:"Kevin Durant"})                                                 |
      | ("Kings":team{name:"Kings"})                                                                        |
      | ("Klay Thompson":player{age:29,name:"Klay Thompson"})                                               |
      | ("Knicks":team{name:"Knicks"})                                                                      |
      | ("Kobe Bryant":player{age:40,name:"Kobe Bryant"})                                                   |
      | ("Kristaps Porzingis":player{age:23,name:"Kristaps Porzingis"})                                     |
      | ("Kyle Anderson":player{age:25,name:"Kyle Anderson"})                                               |
      | ("Kyrie Irving":player{age:26,name:"Kyrie Irving"})                                                 |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Luka Doncic":player{age:20,name:"Luka Doncic"})                                                   |
      | ("Magic":team{name:"Magic"})                                                                        |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Marc Gasol":player{age:34,name:"Marc Gasol"})                                                     |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Nets":team{name:"Nets"})                                                                          |
      | ("Nobody":player{age:0,name:"Nobody"})                                                              |
      | ("Nuggets":team{name:"Nuggets"})                                                                    |
      | ("Null1":player{age:-1,name:NULL})                                                                  |
      | ("Null2":player{age:-2,name:NULL})                                                                  |
      | ("Null3":player{age:-3,name:NULL})                                                                  |
      | ("Null4":player{age:-4,name:NULL})                                                                  |
      | ("Pacers":team{name:"Pacers"})                                                                      |
      | ("Paul Gasol":player{age:38,name:"Paul Gasol"})                                                     |
      | ("Paul George":player{age:28,name:"Paul George"})                                                   |
      | ("Pelicans":team{name:"Pelicans"})                                                                  |
      | ("Pistons":team{name:"Pistons"})                                                                    |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Ray Allen":player{age:43,name:"Ray Allen"})                                                       |
      | ("Ricky Rubio":player{age:28,name:"Ricky Rubio"})                                                   |
      | ("Rockets":team{name:"Rockets"})                                                                    |
      | ("Rudy Gay":player{age:32,name:"Rudy Gay"})                                                         |
      | ("Russell Westbrook":player{age:30,name:"Russell Westbrook"})                                       |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Stephen Curry":player{age:31,name:"Stephen Curry"})                                               |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Thunders":team{name:"Thunders"})                                                                  |
      | ("Tiago Splitter":player{age:34,name:"Tiago Splitter"})                                             |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Timberwolves":team{name:"Timberwolves"})                                                          |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Trail Blazers":team{name:"Trail Blazers"})                                                        |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Warriors":team{name:"Warriors"})                                                                  |
      | ("Wizards":team{name:"Wizards"})                                                                    |
      | ("Yao Ming":player{age:38,name:"Yao Ming"})                                                         |
    When executing query:
      """
      MATCH (v:player:bachelor) RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                   |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
    # TODO(jie): Optimize this case
    When executing query:
      """
      MATCH (v) WHERE v.player.age == 18 RETURN v LIMIT 3
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      MATCH (v:player{age:23}:bachelor) RETURN v
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      MATCH () -[]-> (v) RETURN *
      """
    Then the result should be, in any order:
      | v                                                                                                   |
      | ("Grizzlies":team{name:"Grizzlies"})                                                                |
      | ("Hawks":team{name:"Hawks"})                                                                        |
      | ("Kings":team{name:"Kings"})                                                                        |
      | ("Magic":team{name:"Magic"})                                                                        |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Nets":team{name:"Nets"})                                                                          |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Danny Green":player{age:31,name:"Danny Green"})                                                   |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("76ers":team{name:"76ers"})                                                                        |
      | ("Hawks":team{name:"Hawks"})                                                                        |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Cavaliers":team{name:"Cavaliers"})                                                                |
      | ("Celtics":team{name:"Celtics"})                                                                    |
      | ("Heat":team{name:"Heat"})                                                                          |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("Magic":team{name:"Magic"})                                                                        |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("JaVale McGee":player{age:31,name:"JaVale McGee"})                                                 |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Thunders":team{name:"Thunders"})                                                                  |
      | ("James Harden":player{age:29,name:"James Harden"})                                                 |
      | ("Paul George":player{age:28,name:"Paul George"})                                                   |
      | ("Bucks":team{name:"Bucks"})                                                                        |
      | ("Celtics":team{name:"Celtics"})                                                                    |
      | ("Heat":team{name:"Heat"})                                                                          |
      | ("Thunders":team{name:"Thunders"})                                                                  |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Grizzlies":team{name:"Grizzlies"})                                                                |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Paul Gasol":player{age:38,name:"Paul Gasol"})                                                     |
      | ("Warriors":team{name:"Warriors"})                                                                  |
      | ("Bulls":team{name:"Bulls"})                                                                        |
      | ("Celtics":team{name:"Celtics"})                                                                    |
      | ("Kings":team{name:"Kings"})                                                                        |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Pelicans":team{name:"Pelicans"})                                                                  |
      | ("Ray Allen":player{age:43,name:"Ray Allen"})                                                       |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Dirk Nowitzki":player{age:40,name:"Dirk Nowitzki"})                                               |
      | ("James Harden":player{age:29,name:"James Harden"})                                                 |
      | ("Kristaps Porzingis":player{age:23,name:"Kristaps Porzingis"})                                     |
      | ("Bucks":team{name:"Bucks"})                                                                        |
      | ("Bulls":team{name:"Bulls"})                                                                        |
      | ("Grizzlies":team{name:"Grizzlies"})                                                                |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Kobe Bryant":player{age:40,name:"Kobe Bryant"})                                                   |
      | ("Marc Gasol":player{age:34,name:"Marc Gasol"})                                                     |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Trail Blazers":team{name:"Trail Blazers"})                                                        |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Amar'e Stoudemire":player{age:36,name:"Amar'e Stoudemire"})                                       |
      | ("Dirk Nowitzki":player{age:40,name:"Dirk Nowitzki"})                                               |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Stephen Curry":player{age:31,name:"Stephen Curry"})                                               |
      | ("Cavaliers":team{name:"Cavaliers"})                                                                |
      | ("Celtics":team{name:"Celtics"})                                                                    |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Knicks":team{name:"Knicks"})                                                                      |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Luka Doncic":player{age:20,name:"Luka Doncic"})                                                   |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("Thunders":team{name:"Thunders"})                                                                  |
      | ("Warriors":team{name:"Warriors"})                                                                  |
      | ("Grizzlies":team{name:"Grizzlies"})                                                                |
      | ("Kings":team{name:"Kings"})                                                                        |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("Knicks":team{name:"Knicks"})                                                                      |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Nets":team{name:"Nets"})                                                                          |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Dirk Nowitzki":player{age:40,name:"Dirk Nowitzki"})                                               |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Rockets":team{name:"Rockets"})                                                                    |
      | ("Thunders":team{name:"Thunders"})                                                                  |
      | ("Russell Westbrook":player{age:30,name:"Russell Westbrook"})                                       |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Nuggets":team{name:"Nuggets"})                                                                    |
      | ("Warriors":team{name:"Warriors"})                                                                  |
      | ("Wizards":team{name:"Wizards"})                                                                    |
      | ("Magic":team{name:"Magic"})                                                                        |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Rockets":team{name:"Rockets"})                                                                    |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Grant Hill":player{age:46,name:"Grant Hill"})                                                     |
      | ("Kobe Bryant":player{age:40,name:"Kobe Bryant"})                                                   |
      | ("Rudy Gay":player{age:32,name:"Rudy Gay"})                                                         |
      | ("Clippers":team{name:"Clippers"})                                                                  |
      | ("Magic":team{name:"Magic"})                                                                        |
      | ("Pistons":team{name:"Pistons"})                                                                    |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Bucks":team{name:"Bucks"})                                                                        |
      | ("76ers":team{name:"76ers"})                                                                        |
      | ("Ben Simmons":player{age:22,name:"Ben Simmons"})                                                   |
      | ("Hawks":team{name:"Hawks"})                                                                        |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("Magic":team{name:"Magic"})                                                                        |
      | ("Rockets":team{name:"Rockets"})                                                                    |
      | ("Wizards":team{name:"Wizards"})                                                                    |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
      | ("Grizzlies":team{name:"Grizzlies"})                                                                |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("Danny Green":player{age:31,name:"Danny Green"})                                                   |
      | ("James Harden":player{age:29,name:"James Harden"})                                                 |
      | ("Kevin Durant":player{age:30,name:"Kevin Durant"})                                                 |
      | ("Kyle Anderson":player{age:25,name:"Kyle Anderson"})                                               |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Russell Westbrook":player{age:30,name:"Russell Westbrook"})                                       |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Pacers":team{name:"Pacers"})                                                                      |
      | ("Thunders":team{name:"Thunders"})                                                                  |
      | ("Russell Westbrook":player{age:30,name:"Russell Westbrook"})                                       |
      | ("76ers":team{name:"76ers"})                                                                        |
      | ("Magic":team{name:"Magic"})                                                                        |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Clippers":team{name:"Clippers"})                                                                  |
      | ("Knicks":team{name:"Knicks"})                                                                      |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("Pacers":team{name:"Pacers"})                                                                      |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Warriors":team{name:"Warriors"})                                                                  |
      | ("Jazz":team{name:"Jazz"})                                                                          |
      | ("Timberwolves":team{name:"Timberwolves"})                                                          |
      | ("Trail Blazers":team{name:"Trail Blazers"})                                                        |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("Bulls":team{name:"Bulls"})                                                                        |
      | ("Cavaliers":team{name:"Cavaliers"})                                                                |
      | ("Heat":team{name:"Heat"})                                                                          |
      | ("Heat":team{name:"Heat"})                                                                          |
      | ("Carmelo Anthony":player{age:34,name:"Carmelo Anthony"})                                           |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Cavaliers":team{name:"Cavaliers"})                                                                |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Pacers":team{name:"Pacers"})                                                                      |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("76ers":team{name:"76ers"})                                                                        |
      | ("Bulls":team{name:"Bulls"})                                                                        |
      | ("Hawks":team{name:"Hawks"})                                                                        |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("Kings":team{name:"Kings"})                                                                        |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Warriors":team{name:"Warriors"})                                                                  |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Danny Green":player{age:31,name:"Danny Green"})                                                   |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Clippers":team{name:"Clippers"})                                                                  |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("Rockets":team{name:"Rockets"})                                                                    |
      | ("Carmelo Anthony":player{age:34,name:"Carmelo Anthony"})                                           |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Warriors":team{name:"Warriors"})                                                                  |
      | ("Stephen Curry":player{age:31,name:"Stephen Curry"})                                               |
      | ("Knicks":team{name:"Knicks"})                                                                      |
      | ("Nuggets":team{name:"Nuggets"})                                                                    |
      | ("Rockets":team{name:"Rockets"})                                                                    |
      | ("Thunders":team{name:"Thunders"})                                                                  |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Cavaliers":team{name:"Cavaliers"})                                                                |
      | ("Heat":team{name:"Heat"})                                                                          |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("Cavaliers":team{name:"Cavaliers"})                                                                |
      | ("Ray Allen":player{age:43,name:"Ray Allen"})                                                       |
      | ("Hawks":team{name:"Hawks"})                                                                        |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("Jazz":team{name:"Jazz"})                                                                          |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Clippers":team{name:"Clippers"})                                                                  |
      | ("Pistons":team{name:"Pistons"})                                                                    |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Kyle Anderson":player{age:25,name:"Kyle Anderson"})                                               |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("76ers":team{name:"76ers"})                                                                        |
      | ("Joel Embiid":player{age:25,name:"Joel Embiid"})                                                   |
      | ("Rockets":team{name:"Rockets"})                                                                    |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Celtics":team{name:"Celtics"})                                                                    |
      | ("Pistons":team{name:"Pistons"})                                                                    |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Heat":team{name:"Heat"})                                                                          |
      | ("Knicks":team{name:"Knicks"})                                                                      |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
    When executing query:
      """
      MATCH () --> (v) --> () RETURN count(*)
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
      | p                                                                                                   |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Tiago Splitter":player{age:34,name:"Tiago Splitter"})                                             |
      | ("Spurs":team{name:"Spurs"})                                                                        |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Pistons":team{name:"Pistons"})                                                                    |
      | ("Pacers":team{name:"Pacers"})                                                                      |
      | ("Null4":player{age:-4,name:NULL})                                                                  |
      | ("Russell Westbrook":player{age:30,name:"Russell Westbrook"})                                       |
      | ("Null3":player{age:-3,name:NULL})                                                                  |
      | ("Nuggets":team{name:"Nuggets"})                                                                    |
      | ("Nets":team{name:"Nets"})                                                                          |
      | ("Mavericks":team{name:"Mavericks"})                                                                |
      | ("Ray Allen":player{age:43,name:"Ray Allen"})                                                       |
      | ("Pelicans":team{name:"Pelicans"})                                                                  |
      | ("Marc Gasol":player{age:34,name:"Marc Gasol"})                                                     |
      | ("Stephen Curry":player{age:31,name:"Stephen Curry"})                                               |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Luka Doncic":player{age:20,name:"Luka Doncic"})                                                   |
      | ("Paul Gasol":player{age:38,name:"Paul Gasol"})                                                     |
      | ("Null2":player{age:-2,name:NULL})                                                                  |
      | ("Lakers":team{name:"Lakers"})                                                                      |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
      | ("Kyrie Irving":player{age:26,name:"Kyrie Irving"})                                                 |
      | ("Kristaps Porzingis":player{age:23,name:"Kristaps Porzingis"})                                     |
      | ("Kobe Bryant":player{age:40,name:"Kobe Bryant"})                                                   |
      | ("Knicks":team{name:"Knicks"})                                                                      |
      | ("Kings":team{name:"Kings"})                                                                        |
      | ("Kevin Durant":player{age:30,name:"Kevin Durant"})                                                 |
      | ("Magic":team{name:"Magic"})                                                                        |
      | ("Jazz":team{name:"Jazz"})                                                                          |
      | ("Wizards":team{name:"Wizards"})                                                                    |
      | ("Rudy Gay":player{age:32,name:"Rudy Gay"})                                                         |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Null1":player{age:-1,name:NULL})                                                                  |
      | ("James Harden":player{age:29,name:"James Harden"})                                                 |
      | ("JaVale McGee":player{age:31,name:"JaVale McGee"})                                                 |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Hornets":team{name:"Hornets"})                                                                    |
      | ("Heat":team{name:"Heat"})                                                                          |
      | ("Hawks":team{name:"Hawks"})                                                                        |
      | ("Grizzlies":team{name:"Grizzlies"})                                                                |
      | ("Grant Hill":player{age:46,name:"Grant Hill"})                                                     |
      | ("Timberwolves":team{name:"Timberwolves"})                                                          |
      | ("Giannis Antetokounmpo":player{age:24,name:"Giannis Antetokounmpo"})                               |
      | ("Joel Embiid":player{age:25,name:"Joel Embiid"})                                                   |
      | ("Dwight Howard":player{age:33,name:"Dwight Howard"})                                               |
      | ("Dirk Nowitzki":player{age:40,name:"Dirk Nowitzki"})                                               |
      | ("Kyle Anderson":player{age:25,name:"Kyle Anderson"})                                               |
      | ("Dejounte Murray":player{age:29,name:"Dejounte Murray"})                                           |
      | ("Suns":team{name:"Suns"})                                                                          |
      | ("Paul George":player{age:28,name:"Paul George"})                                                   |
      | ("Nobody":player{age:0,name:"Nobody"})                                                              |
      | ("Jonathon Simmons":player{age:29,name:"Jonathon Simmons"})                                         |
      | ("DeAndre Jordan":player{age:30,name:"DeAndre Jordan"})                                             |
      | ("David West":player{age:38,name:"David West"})                                                     |
      | ("Ricky Rubio":player{age:28,name:"Ricky Rubio"})                                                   |
      | ("Damian Lillard":player{age:28,name:"Damian Lillard"})                                             |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("Danny Green":player{age:31,name:"Danny Green"})                                                   |
      | ("Cory Joseph":player{age:27,name:"Cory Joseph"})                                                   |
      | ("Trail Blazers":team{name:"Trail Blazers"})                                                        |
      | ("Raptors":team{name:"Raptors"})                                                                    |
      | ("Clippers":team{name:"Clippers"})                                                                  |
      | ("Warriors":team{name:"Warriors"})                                                                  |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("Thunders":team{name:"Thunders"})                                                                  |
      | ("Celtics":team{name:"Celtics"})                                                                    |
      | ("Klay Thompson":player{age:29,name:"Klay Thompson"})                                               |
      | ("Cavaliers":team{name:"Cavaliers"})                                                                |
      | ("Carmelo Anthony":player{age:34,name:"Carmelo Anthony"})                                           |
      | ("Rockets":team{name:"Rockets"})                                                                    |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Bulls":team{name:"Bulls"})                                                                        |
      | ("Bucks":team{name:"Bucks"})                                                                        |
      | ("Boris Diaw":player{age:36,name:"Boris Diaw"})                                                     |
      | ("Blake Griffin":player{age:30,name:"Blake Griffin"})                                               |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Ben Simmons":player{age:22,name:"Ben Simmons"})                                                   |
      | ("Yao Ming":player{age:38,name:"Yao Ming"})                                                         |
      | ("Aron Baynes":player{age:32,name:"Aron Baynes"})                                                   |
      | ("Amar'e Stoudemire":player{age:36,name:"Amar'e Stoudemire"})                                       |
      | ("76ers":team{name:"76ers"})                                                                        |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Vince Carter":player{age:42,name:"Vince Carter"})                                                 |
      | ("Tim Duncan":player{age:42,name:"Tim Duncan"}:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
      | ("Tiago Splitter":player{age:34,name:"Tiago Splitter"})                                             |
      | ("Tiago Splitter":player{age:34,name:"Tiago Splitter"})                                             |
      | ("Tiago Splitter":player{age:34,name:"Tiago Splitter"})                                             |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Shaquille O'Neal":player{age:47,name:"Shaquille O'Neal"})                                         |
      | ("Russell Westbrook":player{age:30,name:"Russell Westbrook"})                                       |
      | ("Ray Allen":player{age:43,name:"Ray Allen"})                                                       |
      | ("Ray Allen":player{age:43,name:"Ray Allen"})                                                       |
      | ("Ray Allen":player{age:43,name:"Ray Allen"})                                                       |
      | ("Ray Allen":player{age:43,name:"Ray Allen"})                                                       |
      | ("Marc Gasol":player{age:34,name:"Marc Gasol"})                                                     |
      | ("Marc Gasol":player{age:34,name:"Marc Gasol"})                                                     |
      | ("Stephen Curry":player{age:31,name:"Stephen Curry"})                                               |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Rajon Rondo":player{age:33,name:"Rajon Rondo"})                                                   |
      | ("Manu Ginobili":player{age:41,name:"Manu Ginobili"})                                               |
      | ("Luka Doncic":player{age:20,name:"Luka Doncic"})                                                   |
      | ("Paul Gasol":player{age:38,name:"Paul Gasol"})                                                     |
      | ("Paul Gasol":player{age:38,name:"Paul Gasol"})                                                     |
      | ("Paul Gasol":player{age:38,name:"Paul Gasol"})                                                     |
      | ("Paul Gasol":player{age:38,name:"Paul Gasol"})                                                     |
      | ("Paul Gasol":player{age:38,name:"Paul Gasol"})                                                     |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("LaMarcus Aldridge":player{age:33,name:"LaMarcus Aldridge"})                                       |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
      | ("Steve Nash":player{age:45,name:"Steve Nash"})                                                     |
      | ("Kyrie Irving":player{age:26,name:"Kyrie Irving"})                                                 |
      | ("Kyrie Irving":player{age:26,name:"Kyrie Irving"})                                                 |
      | ("Kristaps Porzingis":player{age:23,name:"Kristaps Porzingis"})                                     |
      | ("Kristaps Porzingis":player{age:23,name:"Kristaps Porzingis"})                                     |
      | ("Kobe Bryant":player{age:40,name:"Kobe Bryant"})                                                   |
      | ("Kevin Durant":player{age:30,name:"Kevin Durant"})                                                 |
      | ("Kevin Durant":player{age:30,name:"Kevin Durant"})                                                 |
      | ("Rudy Gay":player{age:32,name:"Rudy Gay"})                                                         |
      | ("Rudy Gay":player{age:32,name:"Rudy Gay"})                                                         |
      | ("Rudy Gay":player{age:32,name:"Rudy Gay"})                                                         |
      | ("Rudy Gay":player{age:32,name:"Rudy Gay"})                                                         |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("Jason Kidd":player{age:45,name:"Jason Kidd"})                                                     |
      | ("James Harden":player{age:29,name:"James Harden"})                                                 |
      | ("James Harden":player{age:29,name:"James Harden"})                                                 |
      | ("JaVale McGee":player{age:31,name:"JaVale McGee"})                                                 |
      | ("JaVale McGee":player{age:31,name:"JaVale McGee"})                                                 |
      | ("JaVale McGee":player{age:31,name:"JaVale McGee"})                                                 |
      | ("JaVale McGee":player{age:31,name:"JaVale McGee"})                                                 |
      | ("JaVale McGee":player{age:31,name:"JaVale McGee"})                                                 |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Tracy McGrady":player{age:39,name:"Tracy McGrady"})                                               |
      | ("Grant Hill":player{age:46,name:"Grant Hill"})                                                     |
      | ("Grant Hill":player{age:46,name:"Grant Hill"})                                                     |
      | ("Grant Hill":player{age:46,name:"Grant Hill"})                                                     |
      | ("Grant Hill":player{age:46,name:"Grant Hill"})                                                     |
      | ("Giannis Antetokounmpo":player{age:24,name:"Giannis Antetokounmpo"})                               |
      | ("Joel Embiid":player{age:25,name:"Joel Embiid"})                                                   |
      | ("Dwight Howard":player{age:33,name:"Dwight Howard"})                                               |
      | ("Dwight Howard":player{age:33,name:"Dwight Howard"})                                               |
      | ("Dwight Howard":player{age:33,name:"Dwight Howard"})                                               |
      | ("Dwight Howard":player{age:33,name:"Dwight Howard"})                                               |
      | ("Dwight Howard":player{age:33,name:"Dwight Howard"})                                               |
      | ("Dwight Howard":player{age:33,name:"Dwight Howard"})                                               |
      | ("Dirk Nowitzki":player{age:40,name:"Dirk Nowitzki"})                                               |
      | ("Kyle Anderson":player{age:25,name:"Kyle Anderson"})                                               |
      | ("Kyle Anderson":player{age:25,name:"Kyle Anderson"})                                               |
      | ("Dejounte Murray":player{age:29,name:"Dejounte Murray"})                                           |
      | ("Paul George":player{age:28,name:"Paul George"})                                                   |
      | ("Paul George":player{age:28,name:"Paul George"})                                                   |
      | ("Jonathon Simmons":player{age:29,name:"Jonathon Simmons"})                                         |
      | ("Jonathon Simmons":player{age:29,name:"Jonathon Simmons"})                                         |
      | ("Jonathon Simmons":player{age:29,name:"Jonathon Simmons"})                                         |
      | ("DeAndre Jordan":player{age:30,name:"DeAndre Jordan"})                                             |
      | ("DeAndre Jordan":player{age:30,name:"DeAndre Jordan"})                                             |
      | ("DeAndre Jordan":player{age:30,name:"DeAndre Jordan"})                                             |
      | ("David West":player{age:38,name:"David West"})                                                     |
      | ("David West":player{age:38,name:"David West"})                                                     |
      | ("David West":player{age:38,name:"David West"})                                                     |
      | ("David West":player{age:38,name:"David West"})                                                     |
      | ("Ricky Rubio":player{age:28,name:"Ricky Rubio"})                                                   |
      | ("Ricky Rubio":player{age:28,name:"Ricky Rubio"})                                                   |
      | ("Damian Lillard":player{age:28,name:"Damian Lillard"})                                             |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("Dwyane Wade":player{age:37,name:"Dwyane Wade"})                                                   |
      | ("Danny Green":player{age:31,name:"Danny Green"})                                                   |
      | ("Danny Green":player{age:31,name:"Danny Green"})                                                   |
      | ("Danny Green":player{age:31,name:"Danny Green"})                                                   |
      | ("Cory Joseph":player{age:27,name:"Cory Joseph"})                                                   |
      | ("Cory Joseph":player{age:27,name:"Cory Joseph"})                                                   |
      | ("Cory Joseph":player{age:27,name:"Cory Joseph"})                                                   |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Marco Belinelli":player{age:32,name:"Marco Belinelli"})                                           |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("Chris Paul":player{age:33,name:"Chris Paul"})                                                     |
      | ("Klay Thompson":player{age:29,name:"Klay Thompson"})                                               |
      | ("Carmelo Anthony":player{age:34,name:"Carmelo Anthony"})                                           |
      | ("Carmelo Anthony":player{age:34,name:"Carmelo Anthony"})                                           |
      | ("Carmelo Anthony":player{age:34,name:"Carmelo Anthony"})                                           |
      | ("Carmelo Anthony":player{age:34,name:"Carmelo Anthony"})                                           |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("LeBron James":player{age:34,name:"LeBron James"})                                                 |
      | ("Boris Diaw":player{age:36,name:"Boris Diaw"})                                                     |
      | ("Boris Diaw":player{age:36,name:"Boris Diaw"})                                                     |
      | ("Boris Diaw":player{age:36,name:"Boris Diaw"})                                                     |
      | ("Boris Diaw":player{age:36,name:"Boris Diaw"})                                                     |
      | ("Boris Diaw":player{age:36,name:"Boris Diaw"})                                                     |
      | ("Blake Griffin":player{age:30,name:"Blake Griffin"})                                               |
      | ("Blake Griffin":player{age:30,name:"Blake Griffin"})                                               |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Tony Parker":player{age:36,name:"Tony Parker"})                                                   |
      | ("Ben Simmons":player{age:22,name:"Ben Simmons"})                                                   |
      | ("Yao Ming":player{age:38,name:"Yao Ming"})                                                         |
      | ("Aron Baynes":player{age:32,name:"Aron Baynes"})                                                   |
      | ("Aron Baynes":player{age:32,name:"Aron Baynes"})                                                   |
      | ("Aron Baynes":player{age:32,name:"Aron Baynes"})                                                   |
      | ("Amar'e Stoudemire":player{age:36,name:"Amar'e Stoudemire"})                                       |
      | ("Amar'e Stoudemire":player{age:36,name:"Amar'e Stoudemire"})                                       |
      | ("Amar'e Stoudemire":player{age:36,name:"Amar'e Stoudemire"})                                       |

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
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-[]-(v2)-[]-(v3)
      WITH {a : v, b: v3} AS m
      RETURN distinct m.a.player.age AS vage, m.b.team AS bteam
      """
    Then the result should be, in any order, with relax comparison:
      | vage | bteam                   |
      | 42   | NULL                    |
      | 42   | {name: "Suns"}          |
      | 42   | {name: "Magic"}         |
      | 42   | {name: "Spurs"}         |
      | 42   | {name: "Lakers"}        |
      | 42   | {name: "Heat"}          |
      | 42   | {name: "Celtics"}       |
      | 42   | {name: "Cavaliers"}     |
      | 42   | {name: "Hornets"}       |
      | 42   | {name: "Warriors"}      |
      | 42   | {name: "Raptors"}       |
      | 42   | {name: "Kings"}         |
      | 42   | {name: "Hawks"}         |
      | 42   | {name: "Bulls"}         |
      | 42   | {name: "76ers"}         |
      | 42   | {name: "Jazz"}          |
      | 42   | {name: "Trail Blazers"} |
      | 42   | {name: "Pistons"}       |

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
      MATCH (v) WHERE id(v) in ["v1", "v2", "v3", "v4"] RETURN id(v) limit 10;
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
      MATCH (v) WHERE id(v) in ["v1", "v2", "v3", "v4"] RETURN id(v) limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | id(v) |

  Scenario: match with rank
    When executing query:
      """
      match (v)-[e:like]->()
      where id(v) == "Tim Duncan"
      and rank(e) == 0
      RETURN *
      """
    Then the result should be, in any order:
      | v                                                                                                           | e                                                       |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    When executing query:
      """
      match (v)-[e:like]->()
      where id(v) == "Tim Duncan"
      and rank(e) != 0
      RETURN *
      """
    Then the result should be, in any order:
      | v | e |

  Scenario: match with rank
    When executing query:
      """
      match (v)-[e:like]->()
      where id(v) == "Tim Duncan"
      and rank(e) == 0
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                                                                           | e                                                       |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    When executing query:
      """
      match ()-[e]->()
      where rank(e) == 0
      RETURN rank(e)
      limit 3
      """
    Then the result should be, in any order, with relax comparison:
      | rank(e) |
      | 0       |
      | 0       |
      | 0       |
    When executing query:
      """
      match ()-[e]->()
      where rank(e) != 0
      RETURN rank(e)
      limit 1000
      """
    Then the result should be, in any order, with relax comparison:
      | rank(e) |
      | 1       |
      | 1       |
      | 1       |
      | 1       |
      | 1       |
      | 1       |
    When executing query:
      """
      match ()-[e]->()
      where abs(rank(e)) != 0 and e.start_year > 2010
      RETURN rank(e)
      limit 1000
      """
    Then the result should be, in any order, with relax comparison:
      | rank(e) |
      | 1       |
      | 1       |
      | 1       |
      | 1       |
    When executing query:
      """
      match ()-[e]->()
      where abs(rank(e)) == 1 and e.start_year == 2016
      RETURN e
      limit 1000
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                           |
      | [:serve "Marco Belinelli"->"Hornets" @1 {end_year: 2017, start_year: 2016}] |
    When executing query:
      """
      match ()-[e]->()
      where src(e) != "jack"
      RETURN rank(e)
      limit 10000
      """
    Then the result should be, in any order:
      | rank(e) |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
    When executing query:
      """
      match ()-[e]->()
      where src(e) != 0  or abs(rank(e)) != 0
      RETURN rank(e)
      limit 10000
      """
    Then the result should be, in any order:
      | rank(e) |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 1       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |
      | 0       |

  Scenario: match_with_wrong_syntax
    When executing query:
      """
      MATCH (v{name: "Tim Duncan"}) RETURN v
      """
    Then a SemanticError should be raised at runtime: `name:"Tim Duncan"': No tag found for property.
    When executing query:
      """
      MATCH (v:player) RETURN $$.player.age AS a
      """
    Then a SemanticError should be raised at runtime: Expression $$.player.age is not allowed to use in cypher
    When executing query:
      """
      MATCH (v:player) RETURN $^.player.age AS a
      """
    Then a SemanticError should be raised at runtime: Expression $^.player.age is not allowed to use in cypher
    When executing query:
      """
      MATCH (v:player) RETURN $-.player.age AS a
      """
    Then a SemanticError should be raised at runtime: `$-.player', not exist prop `player'
    When executing query:
      """
      MATCH (v:player) WHERE $var.player > 0 RETURN v
      """
    Then a SemanticError should be raised at runtime: `$var.player', not exist variable `var'
    When executing query:
      """
      MATCH (v:player) WHERE $$.player.age > 0 RETURN v
      """
    Then a SemanticError should be raised at runtime: Expression $$.player.age is not allowed to use in cypher
    When executing query:
      """
      MATCH (v:player) WHERE $^.player.age > 0 RETURN v
      """
    Then a SemanticError should be raised at runtime: Expression $^.player.age is not allowed to use in cypher
    When executing query:
      """
      MATCH (v:player) WHERE $-.player.age > 0 RETURN v
      """
    Then a SemanticError should be raised at runtime: `$-.player', not exist prop `player'
    When executing query:
      """
      MATCH (v:player) WHERE $var.player > 0 RETURN v
      """
    Then a SemanticError should be raised at runtime: `$var.player', not exist variable `var'
    When executing query:
      """
      MATCH (v:player) WITH {k: $^} AS x RETUR x.k.player.name
      """
    Then a SyntaxError should be raised at runtime: syntax error near `} AS x R'
    When executing query:
      """
      MATCH (v:player) WITH {k: $^.player.age} AS x RETURN x.k
      """
    Then a SemanticError should be raised at runtime: Expression $^.player.age is not allowed to use in cypher
    When executing query:
      """
      MATCH (v:player) WITH {k: $var} AS x RETUR x.k.player.name
      """
    Then a SyntaxError should be raised at runtime: Direct output of variable is prohibited near `{k: $var}'

  Scenario: match with tag filter
    When executing query:
      """
      MATCH (a:team)-[e*0..1]-(b) where id(a) == 'Tim Duncan' RETURN b
      """
    Then the result should be, in any order, with relax comparison:
      | b |
    When executing query:
      """
      MATCH (a:team)-[e*0..0]-(b) where id(a) in ['Tim Duncan', 'Spurs'] RETURN b
      """
    Then the result should be, in any order, with relax comparison:
      | b         |
      | ('Spurs') |
    When executing query:
      """
      MATCH (a:team)-[e*0..1]-(b) where id(a) in ['Tim Duncan', 'Spurs'] RETURN b
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
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..2]-(v2)
      WHERE size([ii in e WHERE (v)-[ii]-(v2) | ii])>1
      RETURN count(*) AS cnt
      """
    Then the result should be, in any order, with relax comparison:
      | cnt |
      | 0   |
    When executing query:
      """
      MATCH p=(v:player)-[]->() where [ii in relationships(p) where (v)-[ii]->()]
      RETURN count(*) AS cnt
      """
    Then the result should be, in any order, with relax comparison:
      | cnt |
      | 243 |

# Then the result should be, in any order:
# | cnt |
# |  0  |
# When executing query:
# """
# MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..2]-(v2)-[i]-(v3)
# WHERE size([i in e WHERE (v)-[i]-(v2) | i])>1
# RETURN count(*) AS cnt
# """
# FIXME(czp): Fix this case after https://github.com/vesoft-inc/nebula/issues/5289 closed
# Then the result should be, in any order:
# | cnt |
# |  0  |

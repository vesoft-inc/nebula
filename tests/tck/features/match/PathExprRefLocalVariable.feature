# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Path expression reference local defined variables

  Background:
    Given a graph with space named "nba"

  Scenario: In Where
    When executing query:
      """
      MATCH (v:player) WHERE [t in [v] | (v)-[:like]->(t)] RETURN v.player.name AS name
      """
    Then the result should be, in any order:
      | name                    |
      | "Nobody"                |
      | "Amar'e Stoudemire"     |
      | "Russell Westbrook"     |
      | "James Harden"          |
      | "Kobe Bryant"           |
      | "Tracy McGrady"         |
      | "Chris Paul"            |
      | "Boris Diaw"            |
      | "LeBron James"          |
      | "Klay Thompson"         |
      | "Kristaps Porzingis"    |
      | "Jonathon Simmons"      |
      | "Marco Belinelli"       |
      | "Luka Doncic"           |
      | "David West"            |
      | "Tony Parker"           |
      | "Danny Green"           |
      | "Rudy Gay"              |
      | "LaMarcus Aldridge"     |
      | "Tim Duncan"            |
      | "Kevin Durant"          |
      | "Stephen Curry"         |
      | "Ray Allen"             |
      | "Tiago Splitter"        |
      | "DeAndre Jordan"        |
      | "Paul Gasol"            |
      | "Aron Baynes"           |
      | "Cory Joseph"           |
      | "Vince Carter"          |
      | "Marc Gasol"            |
      | "Ricky Rubio"           |
      | "Ben Simmons"           |
      | "Giannis Antetokounmpo" |
      | "Rajon Rondo"           |
      | "Manu Ginobili"         |
      | "Kyrie Irving"          |
      | "Carmelo Anthony"       |
      | "Dwyane Wade"           |
      | "Joel Embiid"           |
      | "Damian Lillard"        |
      | "Yao Ming"              |
      | "Kyle Anderson"         |
      | "Dejounte Murray"       |
      | "Blake Griffin"         |
      | "Steve Nash"            |
      | "Jason Kidd"            |
      | "Dirk Nowitzki"         |
      | "Paul George"           |
      | "Grant Hill"            |
      | "Shaquille O'Neal"      |
      | "JaVale McGee"          |
      | "Dwight Howard"         |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | NULL                    |
    When executing query:
      """
      MATCH (v:player) WHERE [t in [v] | (v)-[:like]->(t)] AND (v)-[:serve]->(:team{name: "Spurs"}) RETURN v.player.name AS name
      """
    Then the result should be, in any order:
      | name                |
      | "Tracy McGrady"     |
      | "Boris Diaw"        |
      | "Jonathon Simmons"  |
      | "Marco Belinelli"   |
      | "David West"        |
      | "Tony Parker"       |
      | "Danny Green"       |
      | "Rudy Gay"          |
      | "LaMarcus Aldridge" |
      | "Tim Duncan"        |
      | "Tiago Splitter"    |
      | "Paul Gasol"        |
      | "Aron Baynes"       |
      | "Cory Joseph"       |
      | "Manu Ginobili"     |
      | "Kyle Anderson"     |
      | "Dejounte Murray"   |
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like]->() WHERE [t in [v] | (v)-->(t)] RETURN v.player.name AS name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
      | "Tim Duncan" |
    When executing query:
      """
      MATCH (v:player)-[e:like]->(n) WHERE (n)-[e]->(:player) RETURN v
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      MATCH (v:player)-[e]->(n) WHERE ()-[e]->(:player) and e.likeness<80 RETURN distinct v.player.name AS vname
      """
    Then the result should be, in any order:
      | vname               |
      | "Kyrie Irving"      |
      | "LaMarcus Aldridge" |
      | "Dirk Nowitzki"     |
      | "Rudy Gay"          |
      | "Danny Green"       |
      | "Blake Griffin"     |
      | "Marco Belinelli"   |
      | "Vince Carter"      |
      | "Rajon Rondo"       |
      | "Ray Allen"         |

  Scenario: In With
    When executing query:
      """
      MATCH (v:player), (t:player{name: "Tim Duncan"}) WITH [t in [t] | (v)-[:teammate]->(t)] AS p RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                                                             |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[<("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>]]     |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[<("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})-[:teammate@0 {end_year: 2016, start_year: 2002}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>]] |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
      | [[]]                                                                                                                                                                                                                          |
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like]->(), (t:team {name: "Spurs"}) WITH [t in [t] | (v)-[:serve]->(t)] AS p RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                               |
      | [[<("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>]] |
      | [[<("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>]] |

  Scenario: In Return
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like]->(), (t:team {name: "Spurs"}) return [t in [t] | (v)-[:serve]->(t)] AS p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                               |
      | [[<("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>]] |
      | [[<("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>]] |
    When executing query:
      """
      MATCH (v:player)-[e]->(n) WITH ()-[e{likeness:80}]->(:player) AS p1, ()-[e]-(:team) AS p2, v.player.name AS vname WHERE size(p1)>0 RETURN distinct * ORDER BY vname
      """
    Then the result should be, in any order, with relax comparison:
      | p1                                                                                                                                                                                                        | p2 | vname              |
      | [<("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:like@0 {likeness: 80}]->("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"})>]           | [] | "Aron Baynes"      |
      | [<("Ben Simmons" :player{age: 22, name: "Ben Simmons"})-[:like@0 {likeness: 80}]->("Joel Embiid" :player{age: 25, name: "Joel Embiid"})>]                                                                 | [] | "Ben Simmons"      |
      | [<("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:like@0 {likeness: 80}]->("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"})>]             | [] | "Boris Diaw"       |
      | [<("Boris Diaw" :player{age: 36, name: "Boris Diaw"})-[:like@0 {likeness: 80}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>]                                                                   | [] | "Boris Diaw"       |
      | [<("Damian Lillard" :player{age: 28, name: "Damian Lillard"})-[:like@0 {likeness: 80}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})>]                                               | [] | "Damian Lillard"   |
      | [<("Danny Green" :player{age: 31, name: "Danny Green"})-[:like@0 {likeness: 80}]->("LeBron James" :player{age: 34, name: "LeBron James"})>]                                                               | [] | "Danny Green"      |
      | [<("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})-[:like@0 {likeness: 80}]->("Steve Nash" :player{age: 45, name: "Steve Nash"})>]                                                               | [] | "Dirk Nowitzki"    |
      | [<("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})-[:like@0 {likeness: 80}]->("Jason Kidd" :player{age: 45, name: "Jason Kidd"})>]                                                               | [] | "Dirk Nowitzki"    |
      | [<("James Harden" :player{age: 29, name: "James Harden"})-[:like@0 {likeness: 80}]->("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})>]                                                   | [] | "James Harden"     |
      | [<("Jason Kidd" :player{age: 45, name: "Jason Kidd"})-[:like@0 {likeness: 80}]->("Vince Carter" :player{age: 42, name: "Vince Carter"})>]                                                                 | [] | "Jason Kidd"       |
      | [<("Joel Embiid" :player{age: 25, name: "Joel Embiid"})-[:like@0 {likeness: 80}]->("Ben Simmons" :player{age: 22, name: "Ben Simmons"})>]                                                                 | [] | "Joel Embiid"      |
      | [<("Luka Doncic" :player{age: 20, name: "Luka Doncic"})-[:like@0 {likeness: 80}]->("James Harden" :player{age: 29, name: "James Harden"})>]                                                               | [] | "Luka Doncic"      |
      | [<("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"})>] | [] | "Shaquille O'Neal" |
      | [<("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})-[:like@0 {likeness: 80}]->("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"})>]     | [] | "Tiago Splitter"   |

  Scenario: In Unwind
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like]->(), (t:team {name: "Spurs"}) UNWIND [t in [t] | (v)-[:serve]->(t)] AS p RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                             |
      | [<("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>] |
      | [<("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>] |
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e:like*1..3]->(n), (t:team {name: "Spurs"}) WITH v, e, collect(distinct n) AS ns UNWIND [n in ns | ()-[e*2..4]->(n:player)] AS p RETURN count(p) AS count
      """
    Then the result should be, in any order:
      | count |
      | 11    |

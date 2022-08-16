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

  Scenario: In Unwind
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like]->(), (t:team {name: "Spurs"}) UNWIND [t in [t] | (v)-[:serve]->(t)] AS p RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                             |
      | [<("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>] |
      | [<("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>] |

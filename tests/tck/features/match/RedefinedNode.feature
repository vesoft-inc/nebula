# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Redefined symbols
  Examples:
    | space_name  |
    | nba         |
    | nba_int_vid |

  Background:
    Given a graph with space named "<space_name>"

  Scenario: Redefined node alias
    When executing query:
      """
      match (v:player)-[:like]->(v) return v.player.name AS name
      """
    Then the result should be, in any order:
      | name |
    When executing query:
      """
      match (v)-[:serve]->(t)<-[:serve]-(v) return t.team.name, v.player.name
      """
    Then the result should be, in any order:
      | t.team.name | v.player.name     |
      | "Mavericks" | "Jason Kidd"      |
      | "Mavericks" | "Jason Kidd"      |
      | "Spurs"     | "Marco Belinelli" |
      | "Spurs"     | "Marco Belinelli" |
      | "Heat"      | "Dwyane Wade"     |
      | "Heat"      | "Dwyane Wade"     |
      | "Suns"      | "Steve Nash"      |
      | "Suns"      | "Steve Nash"      |
      | "Hornets"   | "Marco Belinelli" |
      | "Hornets"   | "Marco Belinelli" |
      | "Cavaliers" | "LeBron James"    |
      | "Cavaliers" | "LeBron James"    |
    When executing query:
      """
      match (v)-[]->(t)<-[]-(v:player) return v.player.name, t.team.name
      """
    Then the result should be, in any order:
      | v.player.name     | t.team.name |
      | "LeBron James"    | "Cavaliers" |
      | "LeBron James"    | "Cavaliers" |
      | "Marco Belinelli" | "Hornets"   |
      | "Marco Belinelli" | "Spurs"     |
      | "Marco Belinelli" | "Hornets"   |
      | "Marco Belinelli" | "Spurs"     |
      | "Tony Parker"     | NULL        |
      | "Tony Parker"     | NULL        |
      | "Tony Parker"     | NULL        |
      | "Tony Parker"     | NULL        |
      | "Tony Parker"     | NULL        |
      | "Tony Parker"     | NULL        |
      | "Tim Duncan"      | NULL        |
      | "Tim Duncan"      | NULL        |
      | "Tim Duncan"      | NULL        |
      | "Tim Duncan"      | NULL        |
      | "Manu Ginobili"   | NULL        |
      | "Manu Ginobili"   | NULL        |
      | "Dwyane Wade"     | "Heat"      |
      | "Dwyane Wade"     | "Heat"      |
      | "Steve Nash"      | "Suns"      |
      | "Steve Nash"      | "Suns"      |
      | "Jason Kidd"      | "Mavericks" |
      | "Jason Kidd"      | "Mavericks" |
    When executing query:
      """
      match (v)-[]->(t)<-[:serve]-(v) return t.team.name, v.player.name
      """
    Then the result should be, in any order:
      | t.team.name | v.player.name     |
      | "Mavericks" | "Jason Kidd"      |
      | "Mavericks" | "Jason Kidd"      |
      | "Spurs"     | "Marco Belinelli" |
      | "Spurs"     | "Marco Belinelli" |
      | "Heat"      | "Dwyane Wade"     |
      | "Heat"      | "Dwyane Wade"     |
      | "Suns"      | "Steve Nash"      |
      | "Suns"      | "Steve Nash"      |
      | "Hornets"   | "Marco Belinelli" |
      | "Hornets"   | "Marco Belinelli" |
      | "Cavaliers" | "LeBron James"    |
      | "Cavaliers" | "LeBron James"    |

  Scenario: Redefined node alias in variable steps
    When executing query:
      """
      match (v:player)-[:like*0..2]->(v) return v.player.name AS name
      """
    Then the result should be, in any order:
      | name                    |
      | NULL                    |
      | "LeBron James"          |
      | "LaMarcus Aldridge"     |
      | "Kevin Durant"          |
      | "JaVale McGee"          |
      | "Aron Baynes"           |
      | "Joel Embiid"           |
      | "Jonathon Simmons"      |
      | "Marco Belinelli"       |
      | "Dwight Howard"         |
      | "Shaquille O'Neal"      |
      | "Boris Diaw"            |
      | "Chris Paul"            |
      | NULL                    |
      | "James Harden"          |
      | "Russell Westbrook"     |
      | "Blake Griffin"         |
      | "Kyle Anderson"         |
      | "Dejounte Murray"       |
      | "Kristaps Porzingis"    |
      | "Klay Thompson"         |
      | "Dwyane Wade"           |
      | "Tracy McGrady"         |
      | "Kobe Bryant"           |
      | "Danny Green"           |
      | "Ricky Rubio"           |
      | "Vince Carter"          |
      | "Marc Gasol"            |
      | "Tony Parker"           |
      | "Carmelo Anthony"       |
      | "Tiago Splitter"        |
      | "DeAndre Jordan"        |
      | "Paul Gasol"            |
      | "Rajon Rondo"           |
      | "Amar'e Stoudemire"     |
      | "Cory Joseph"           |
      | "Manu Ginobili"         |
      | "Paul George"           |
      | "Grant Hill"            |
      | "Ray Allen"             |
      | "Stephen Curry"         |
      | "Steve Nash"            |
      | "Tim Duncan"            |
      | "Damian Lillard"        |
      | NULL                    |
      | "Yao Ming"              |
      | "Nobody"                |
      | "David West"            |
      | "Luka Doncic"           |
      | "Jason Kidd"            |
      | "Dirk Nowitzki"         |
      | "Rudy Gay"              |
      | "Kyrie Irving"          |
      | NULL                    |
      | "Giannis Antetokounmpo" |
      | "Ben Simmons"           |
      | "Rajon Rondo"           |
      | "James Harden"          |
      | "Russell Westbrook"     |
      | "Russell Westbrook"     |
      | "Amar'e Stoudemire"     |
      | "Tony Parker"           |
      | "Tony Parker"           |
      | "Danny Green"           |
      | "Marc Gasol"            |
      | "Manu Ginobili"         |
      | "Ray Allen"             |
      | "Ben Simmons"           |
      | "Dirk Nowitzki"         |
      | "Dirk Nowitzki"         |
      | "Steve Nash"            |
      | "Steve Nash"            |
      | "Steve Nash"            |
      | "Grant Hill"            |
      | "Tim Duncan"            |
      | "Tim Duncan"            |
      | "Paul George"           |
      | "Paul Gasol"            |
      | "Marco Belinelli"       |
      | "Tracy McGrady"         |
      | "Vince Carter"          |
      | "Jason Kidd"            |
      | "Jason Kidd"            |
      | "Jason Kidd"            |
      | "Dwyane Wade"           |
      | "Dwyane Wade"           |
      | "Carmelo Anthony"       |
      | "Carmelo Anthony"       |
      | "Chris Paul"            |
      | "Chris Paul"            |
      | "Kristaps Porzingis"    |
      | "LaMarcus Aldridge"     |
      | "Luka Doncic"           |
      | "Joel Embiid"           |

  Scenario: Redefined edge alias
    When executing query:
      """
      MATCH (v:player{name:"abc"})-[e:like]->(v1)-[e:like]->(v2) RETURN *
      """
    Then a SemanticError should be raised at runtime: `e': Redefined alias

  Scenario: Redefined alias in with
    Given an empty graph
    And load "nba" csv data to a new space
    And having executed:
      """
      insert edge like (likeness) values "Tim Duncan"->"Tim Duncan":(100);
      insert edge like (likeness) values "Carmelo Anthony"->"Carmelo Anthony":(100);
      """
    When executing query:
      """
      MATCH (n0)-[e0]->(n0) WHERE (id(n0) IN ["Tim Duncan", "Carmelo Anthony" ]) with * RETURN *
      """
    Then the result should be, in any order:
      | n0                                                                                                          | e0                                                              |
      | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) | [:like "Tim Duncan"->"Tim Duncan" @0 {likeness: 100}]           |
      | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})                                               | [:like "Carmelo Anthony"->"Carmelo Anthony" @0 {likeness: 100}] |

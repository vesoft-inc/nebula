Feature: Match seek by id

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: basic
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == 'Paul Gasol'
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: basic logical not
    When executing query:
      """
      MATCH (v)
      WHERE NOT NOT id(v) == 'Paul Gasol'
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    When executing query:
      """
      MATCH (v)
      WHERE NOT NOT id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: basic logical and
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | 'Paul Gasol' |

  Scenario: basic logical or
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') OR id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'Paul Gasol'       |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') OR id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'Paul Gasol'       |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: basic logical with noise
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) == 'Paul Gasol'
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | 'Paul Gasol' |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) != 'Paul Gasol'
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
            OR false
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'Paul Gasol'       |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
            OR true
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name                    |
      | NULL                    |
      | "Amar'e Stoudemire"     |
      | "Aron Baynes"           |
      | "Ben Simmons"           |
      | "Blake Griffin"         |
      | "Boris Diaw"            |
      | NULL                    |
      | NULL                    |
      | "Carmelo Anthony"       |
      | NULL                    |
      | NULL                    |
      | "Chris Paul"            |
      | NULL                    |
      | "Cory Joseph"           |
      | "Damian Lillard"        |
      | "Danny Green"           |
      | "David West"            |
      | "DeAndre Jordan"        |
      | "Dejounte Murray"       |
      | "Dirk Nowitzki"         |
      | "Dwight Howard"         |
      | "Dwyane Wade"           |
      | "Giannis Antetokounmpo" |
      | "Grant Hill"            |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | "JaVale McGee"          |
      | "James Harden"          |
      | "Jason Kidd"            |
      | NULL                    |
      | "Joel Embiid"           |
      | "Jonathon Simmons"      |
      | "Kevin Durant"          |
      | NULL                    |
      | "Klay Thompson"         |
      | NULL                    |
      | "Kobe Bryant"           |
      | "Kristaps Porzingis"    |
      | "Kyle Anderson"         |
      | "Kyrie Irving"          |
      | "LaMarcus Aldridge"     |
      | NULL                    |
      | "LeBron James"          |
      | "Luka Doncic"           |
      | NULL                    |
      | "Manu Ginobili"         |
      | "Marc Gasol"            |
      | "Marco Belinelli"       |
      | NULL                    |
      | NULL                    |
      | "Nobody"                |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | "Paul Gasol"            |
      | "Paul George"           |
      | NULL                    |
      | NULL                    |
      | "Rajon Rondo"           |
      | NULL                    |
      | "Ray Allen"             |
      | "Ricky Rubio"           |
      | NULL                    |
      | "Rudy Gay"              |
      | "Russell Westbrook"     |
      | "Shaquille O'Neal"      |
      | NULL                    |
      | "Stephen Curry"         |
      | "Steve Nash"            |
      | NULL                    |
      | NULL                    |
      | "Tiago Splitter"        |
      | "Tim Duncan"            |
      | NULL                    |
      | "Tony Parker"           |
      | "Tracy McGrady"         |
      | NULL                    |
      | "Vince Carter"          |
      | NULL                    |
      | NULL                    |
      | "Yao Ming"              |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
            AND true
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'Paul Gasol'       |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
            AND (id(v) == 'James Harden' OR v.player.age == 23)
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name           |
      | 'James Harden' |
    When executing query:
      """
      MATCH (v:player)
      WHERE id(v) IN ['James Harden', v.player.age]
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name           |
      | 'James Harden' |

  Scenario: complicate logical
    When executing query:
      """
      MATCH (v)
      WHERE ((NOT NOT id(v) == 'Paul Gasol')
            OR id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray'])
            AND id(v) != 'Paul Gasol'
            AND v.player.name != 'Jonathon Simmons'
            AND v.player.age == 29
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name              |
      | 'James Harden'    |
      | 'Klay Thompson'   |
      | 'Dejounte Murray' |
    When executing query:
      """
      MATCH (v)
      WHERE (id(v) == "Tim Duncan" AND v.player.age>10) OR (id(v) == "Tony Parker" AND v.player.age>10)
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name          |
      | "Tim Duncan"  |
      | "Tony Parker" |

  Scenario: with extend
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) == 'Paul Gasol'
      RETURN v.player.name AS Name, t.team.name AS Team
      """
    Then the result should be, in any order:
      | Name         | Team        |
      | 'Paul Gasol' | 'Grizzlies' |
      | 'Paul Gasol' | 'Lakers'    |
      | 'Paul Gasol' | 'Bulls'     |
      | 'Paul Gasol' | 'Spurs'     |
      | 'Paul Gasol' | 'Bucks'     |

  Scenario: multiple nodes
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) == 'Paul Gasol' AND id(t) IN ['Grizzlies', 'Lakers']
      RETURN v.player.name AS Name, t.team.name AS Team
      """
    Then the result should be, in any order:
      | Name         | Team        |
      | 'Paul Gasol' | 'Grizzlies' |
      | 'Paul Gasol' | 'Lakers'    |
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE ((NOT NOT id(v) == 'Paul Gasol') AND id(v) == 'Paul Gasol') OR id(t) IN ['Grizzlies', 'Lakers']
      RETURN v.player.name AS Name, t.team.name AS Team
      """
    Then the result should be, in any order:
      | Name               | Team        |
      | "Paul Gasol"       | "Bucks"     |
      | "Paul Gasol"       | "Bulls"     |
      | "Rudy Gay"         | "Grizzlies" |
      | "Kyle Anderson"    | "Grizzlies" |
      | "Paul Gasol"       | "Grizzlies" |
      | "Marc Gasol"       | "Grizzlies" |
      | "Vince Carter"     | "Grizzlies" |
      | "Paul Gasol"       | "Spurs"     |
      | "Dwight Howard"    | "Lakers"    |
      | "Shaquille O'Neal" | "Lakers"    |
      | "Steve Nash"       | "Lakers"    |
      | "Paul Gasol"       | "Lakers"    |
      | "Kobe Bryant"      | "Lakers"    |
      | "JaVale McGee"     | "Lakers"    |
      | "Rajon Rondo"      | "Lakers"    |
      | "LeBron James"     | "Lakers"    |

  Scenario: can't refer
    When executing query:
      """
      WITH [1, 2, 3] AS coll
      UNWIND coll AS vid
      MATCH (v) WHERE id(v) == "Tony Parker" OR id(v) == vid
      RETURN v;
      """
    Then the result should be, in any order:
      | v                                                 |
      | ("Tony Parker":player{age:36,name:"Tony Parker"}) |
      | ("Tony Parker":player{age:36,name:"Tony Parker"}) |
      | ("Tony Parker":player{age:36,name:"Tony Parker"}) |
    # start vid finder don't support variable currently
    When executing query:
      """
      WITH [1, 2, 3] AS coll
      UNWIND coll AS vid
      MATCH (v) WHERE id(v) == vid
      RETURN v;
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) == 'Paul Gasol'
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then the result should be, in any order:
      | Name                    | Age  |
      | NULL                    | NULL |
      | "Amar'e Stoudemire"     | 36   |
      | "Aron Baynes"           | 32   |
      | "Ben Simmons"           | 22   |
      | "Blake Griffin"         | 30   |
      | "Boris Diaw"            | 36   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Carmelo Anthony"       | 34   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Chris Paul"            | 33   |
      | NULL                    | NULL |
      | "Cory Joseph"           | 27   |
      | "Damian Lillard"        | 28   |
      | "Danny Green"           | 31   |
      | "David West"            | 38   |
      | "DeAndre Jordan"        | 30   |
      | "Dejounte Murray"       | 29   |
      | "Dirk Nowitzki"         | 40   |
      | "Dwight Howard"         | 33   |
      | "Dwyane Wade"           | 37   |
      | "Giannis Antetokounmpo" | 24   |
      | "Grant Hill"            | 46   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "JaVale McGee"          | 31   |
      | "James Harden"          | 29   |
      | "Jason Kidd"            | 45   |
      | NULL                    | NULL |
      | "Joel Embiid"           | 25   |
      | "Jonathon Simmons"      | 29   |
      | "Kevin Durant"          | 30   |
      | NULL                    | NULL |
      | "Klay Thompson"         | 29   |
      | NULL                    | NULL |
      | "Kobe Bryant"           | 40   |
      | "Kristaps Porzingis"    | 23   |
      | "Kyle Anderson"         | 25   |
      | "Kyrie Irving"          | 26   |
      | "LaMarcus Aldridge"     | 33   |
      | NULL                    | NULL |
      | "LeBron James"          | 34   |
      | "Luka Doncic"           | 20   |
      | NULL                    | NULL |
      | "Manu Ginobili"         | 41   |
      | "Marc Gasol"            | 34   |
      | "Marco Belinelli"       | 32   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Nobody"                | 0    |
      | NULL                    | NULL |
      | NULL                    | -1   |
      | NULL                    | -2   |
      | NULL                    | -3   |
      | NULL                    | -4   |
      | NULL                    | NULL |
      | "Yao Ming"              | 38   |
      | "Paul George"           | 28   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Rajon Rondo"           | 33   |
      | NULL                    | NULL |
      | "Ray Allen"             | 43   |
      | "Ricky Rubio"           | 28   |
      | NULL                    | NULL |
      | "Rudy Gay"              | 32   |
      | "Russell Westbrook"     | 30   |
      | "Shaquille O'Neal"      | 47   |
      | NULL                    | NULL |
      | "Stephen Curry"         | 31   |
      | "Steve Nash"            | 45   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Tiago Splitter"        | 34   |
      | "Tim Duncan"            | 42   |
      | NULL                    | NULL |
      | "Tony Parker"           | 36   |
      | "Tracy McGrady"         | 39   |
      | NULL                    | NULL |
      | "Vince Carter"          | 42   |
      | NULL                    | NULL |
      | NULL                    | NULL |
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name                    |
      | NULL                    |
      | NULL                    |
      | "Kyrie Irving"          |
      | NULL                    |
      | "Grant Hill"            |
      | "Tracy McGrady"         |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | "Joel Embiid"           |
      | "Marc Gasol"            |
      | "Cory Joseph"           |
      | "Giannis Antetokounmpo" |
      | "Aron Baynes"           |
      | "DeAndre Jordan"        |
      | NULL                    |
      | "JaVale McGee"          |
      | NULL                    |
      | "Nobody"                |
      | "Kevin Durant"          |
      | "Jason Kidd"            |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | "Kristaps Porzingis"    |
      | "Dirk Nowitzki"         |
      | "LaMarcus Aldridge"     |
      | "Kobe Bryant"           |
      | NULL                    |
      | NULL                    |
      | "Boris Diaw"            |
      | "Ray Allen"             |
      | "Dwight Howard"         |
      | NULL                    |
      | "Yao Ming"              |
      | "Chris Paul"            |
      | "LeBron James"          |
      | "Shaquille O'Neal"      |
      | NULL                    |
      | NULL                    |
      | "Stephen Curry"         |
      | "Vince Carter"          |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | "Kyle Anderson"         |
      | "Blake Griffin"         |
      | NULL                    |
      | NULL                    |
      | "Tiago Splitter"        |
      | NULL                    |
      | "Rudy Gay"              |
      | "David West"            |
      | "Steve Nash"            |
      | "Dwyane Wade"           |
      | "Manu Ginobili"         |
      | "Ben Simmons"           |
      | NULL                    |
      | "Danny Green"           |
      | NULL                    |
      | "Rajon Rondo"           |
      | NULL                    |
      | "Russell Westbrook"     |
      | NULL                    |
      | NULL                    |
      | "Tony Parker"           |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | NULL                    |
      | "Carmelo Anthony"       |
      | "Damian Lillard"        |
      | NULL                    |
      | "Marco Belinelli"       |
      | "Paul Gasol"            |
      | "Amar'e Stoudemire"     |
      | "Paul George"           |
      | "Luka Doncic"           |
      | "Tim Duncan"            |
      | "Ricky Rubio"           |
      | NULL                    |
    When executing query:
      """
      MATCH (v)
      WHERE id(x) == 'James Harden'
      RETURN v.player.name AS Name
      """
    Then a SemanticError should be raised at runtime: Alias used but not defined: `x'
    When executing query:
      """
      MATCH (v)
      WHERE (id(v) + '') == 'James Harden'
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name           |
      | 'James Harden' |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', v.player.name]
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name                    |
      | "Yao Ming"              |
      | "Amar'e Stoudemire"     |
      | "Aron Baynes"           |
      | "Ben Simmons"           |
      | "Blake Griffin"         |
      | "Boris Diaw"            |
      | "Vince Carter"          |
      | "Tracy McGrady"         |
      | "Carmelo Anthony"       |
      | "Tony Parker"           |
      | "Tim Duncan"            |
      | "Chris Paul"            |
      | "Tiago Splitter"        |
      | "Cory Joseph"           |
      | "Damian Lillard"        |
      | "Danny Green"           |
      | "David West"            |
      | "DeAndre Jordan"        |
      | "Dejounte Murray"       |
      | "Dirk Nowitzki"         |
      | "Dwight Howard"         |
      | "Dwyane Wade"           |
      | "Giannis Antetokounmpo" |
      | "Grant Hill"            |
      | "Steve Nash"            |
      | "Stephen Curry"         |
      | "Shaquille O'Neal"      |
      | "Russell Westbrook"     |
      | "JaVale McGee"          |
      | "James Harden"          |
      | "Jason Kidd"            |
      | "Rudy Gay"              |
      | "Joel Embiid"           |
      | "Jonathon Simmons"      |
      | "Kevin Durant"          |
      | "Ricky Rubio"           |
      | "Klay Thompson"         |
      | "Ray Allen"             |
      | "Kobe Bryant"           |
      | "Kristaps Porzingis"    |
      | "Kyle Anderson"         |
      | "Kyrie Irving"          |
      | "LaMarcus Aldridge"     |
      | "Rajon Rondo"           |
      | "LeBron James"          |
      | "Luka Doncic"           |
      | "Paul George"           |
      | "Manu Ginobili"         |
      | "Marc Gasol"            |
      | "Marco Belinelli"       |
      | "Paul Gasol"            |
      | "Nobody"                |
    When executing query:
      """
      MATCH (v) WHERE id(v) == "Tim Duncan" OR id(v) != "Tony Parker" RETURN COUNT(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 85    |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
            OR v.player.age == 23
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name                 |
      | "Kristaps Porzingis" |
      | "Klay Thompson"      |
      | "Jonathon Simmons"   |
      | "James Harden"       |
      | "Dejounte Murray"    |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == 'James Harden'
            OR v.player.age == 23
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name                 |
      | "Kristaps Porzingis" |
      | "James Harden"       |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == 'James Harden'
            OR v.player.age != 23
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name                    |
      | "Yao Ming"              |
      | "Amar'e Stoudemire"     |
      | "Aron Baynes"           |
      | "Ben Simmons"           |
      | "Blake Griffin"         |
      | "Boris Diaw"            |
      | "Vince Carter"          |
      | "Tracy McGrady"         |
      | "Carmelo Anthony"       |
      | "Tony Parker"           |
      | "Tim Duncan"            |
      | "Chris Paul"            |
      | "Tiago Splitter"        |
      | "Cory Joseph"           |
      | "Damian Lillard"        |
      | "Danny Green"           |
      | "David West"            |
      | "DeAndre Jordan"        |
      | "Dejounte Murray"       |
      | "Dirk Nowitzki"         |
      | "Dwight Howard"         |
      | "Dwyane Wade"           |
      | "Giannis Antetokounmpo" |
      | "Grant Hill"            |
      | "Steve Nash"            |
      | "Stephen Curry"         |
      | "Shaquille O'Neal"      |
      | "Russell Westbrook"     |
      | "JaVale McGee"          |
      | "James Harden"          |
      | "Jason Kidd"            |
      | "Rudy Gay"              |
      | "Joel Embiid"           |
      | "Jonathon Simmons"      |
      | "Kevin Durant"          |
      | "Ricky Rubio"           |
      | "Klay Thompson"         |
      | "Ray Allen"             |
      | "Kobe Bryant"           |
      | "Rajon Rondo"           |
      | "Kyle Anderson"         |
      | "Kyrie Irving"          |
      | "LaMarcus Aldridge"     |
      | "Paul George"           |
      | "LeBron James"          |
      | "Luka Doncic"           |
      | "Paul Gasol"            |
      | "Manu Ginobili"         |
      | "Marc Gasol"            |
      | "Marco Belinelli"       |
      | NULL                    |
      | NULL                    |
      | "Nobody"                |
      | NULL                    |
      | NULL                    |

  Scenario: test OR logic
    When executing query:
      """
      MATCH (v:player)
      WHERE v.player.name == "Tim Duncan"
            OR v.player.age == 23
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                           |
      | ("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"})                                         |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.player.name == "Tim Duncan"
            OR v.noexist.age == 23
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                           |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.player.noexist == "Tim Duncan"
            OR v.player.age == 23
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                   |
      | ("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"}) |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.player.noexist == "Tim Duncan"
            OR v.noexist.age == 23
      RETURN v
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      MATCH (v:player)
      WHERE "Tim Duncan" == v.player.name
            OR 23 + 1 == v.noexist.age - 3
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                           |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
            OR id(v) == 'Yao Ming'
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                               |
      | ("James Harden" :player{age: 29, name: "James Harden"})         |
      | ("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"}) |
      | ("Klay Thompson" :player{age: 29, name: "Klay Thompson"})       |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})   |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                 |

  Scenario: Start from end
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE id(t) == 'Pistons'
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name            |
      | 'Aron Baynes'   |
      | 'Blake Griffin' |
      | 'Grant Hill'    |

  Scenario: check plan
    When profiling query:
      """
      MATCH (v)
      WHERE id(v) == 'Paul Gasol'
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 12 | Project        | 2            |               |
      | 2  | AppendVertices | 16           |               |
      | 16 | Dedup          | 7            |               |
      | 7  | PassThrough    | 0            |               |
      | 0  | Start          |              |               |
    When profiling query:
      """
      MATCH (v)
      WHERE id(v) IN ['Paul Gasol']
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 12 | Project        | 2            |               |
      | 2  | AppendVertices | 16           |               |
      | 16 | Dedup          | 7            |               |
      | 7  | PassThrough    | 0            |               |
      | 0  | Start          |              |               |

  # issue: https://github.com/vesoft-inc/nebula/issues/4417
  Scenario: vid within split function
    When profiling query:
      """
      MATCH (v:player)--(v2)
      WHERE id(v2) IN split("Tim Duncan,player102", ",") and v2.player.age>34
      RETURN v2;
      """
    Then the result should be, in any order:
      | v2                                                                                                          |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 11 | Project        | 8            |               |
      | 8  | Filter         | 4            |               |
      | 4  | AppendVertices | 10           |               |
      | 10 | Filter         | 10           |               |
      | 10 | Traverse       | 2            |               |
      | 2  | Dedup          | 4            |               |
      | 1  | PassThrough    | 3            |               |
      | 3  | Start          |              |               |

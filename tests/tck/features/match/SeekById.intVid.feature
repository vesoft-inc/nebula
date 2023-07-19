Feature: Match seek by id

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: basic
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == hash('Paul Gasol')
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
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
      WHERE NOT NOT id(v) == hash('Paul Gasol')
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    When executing query:
      """
      MATCH (v)
      WHERE NOT NOT id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
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
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) AND id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) AND id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray'), hash('Paul Gasol')]
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | 'Paul Gasol' |

  Scenario: basic logical or
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) OR id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
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
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) OR id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray'), hash('Paul Gasol')]
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
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) AND id(v) == hash('Paul Gasol')
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | 'Paul Gasol' |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) AND id(v) != hash('Paul Gasol')
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray'), hash('Paul Gasol')]
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
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray'), hash('Paul Gasol')]
            AND (id(v) == hash('James Harden') OR v.player.age == 23)
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name           |
      | 'James Harden' |
    When executing query:
      """
      MATCH (v:player)
      WHERE id(v) IN [hash('James Harden'), v.player.age]
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name           |
      | 'James Harden' |

  Scenario: complicate logical
    When executing query:
      """
      MATCH (v)
      WHERE ((NOT NOT id(v) == hash('Paul Gasol'))
            OR id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')])
            AND id(v) != hash('Paul Gasol')
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
      WHERE (id(v) == hash("Tim Duncan") AND v.player.age>10) OR (id(v) == hash("Tony Parker") AND v.player.age>10)
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
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) AND id(v) == hash('Paul Gasol')
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
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) AND id(v) == hash('Paul Gasol') AND id(t) IN [hash('Grizzlies'), hash('Lakers')]
      RETURN v.player.name AS Name, t.team.name AS Team
      """
    Then the result should be, in any order:
      | Name         | Team        |
      | 'Paul Gasol' | 'Grizzlies' |
      | 'Paul Gasol' | 'Lakers'    |
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE ((NOT NOT id(v) == hash('Paul Gasol')) AND id(v) == hash('Paul Gasol')) OR id(t) IN [hash('Grizzlies'), hash('Lakers')]
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
      MATCH (v)
      WHERE NOT id(v) == hash('Paul Gasol')
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then the result should be, in any order:
      | Name                    | Age  |
      | NULL                    | NULL |
      | NULL                    | -1   |
      | "Kyrie Irving"          | 26   |
      | "James Harden"          | 29   |
      | "Grant Hill"            | 46   |
      | "Tracy McGrady"         | 39   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Joel Embiid"           | 25   |
      | "Marc Gasol"            | 34   |
      | "Cory Joseph"           | 27   |
      | "Giannis Antetokounmpo" | 24   |
      | "Aron Baynes"           | 32   |
      | "DeAndre Jordan"        | 30   |
      | NULL                    | NULL |
      | "JaVale McGee"          | 31   |
      | NULL                    | NULL |
      | "Nobody"                | 0    |
      | "Kevin Durant"          | 30   |
      | "Jason Kidd"            | 45   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Kristaps Porzingis"    | 23   |
      | "Dirk Nowitzki"         | 40   |
      | "LaMarcus Aldridge"     | 33   |
      | "Kobe Bryant"           | 40   |
      | NULL                    | NULL |
      | NULL                    | -3   |
      | "Boris Diaw"            | 36   |
      | "Ray Allen"             | 43   |
      | "Dwight Howard"         | 33   |
      | NULL                    | NULL |
      | "Yao Ming"              | 38   |
      | "Chris Paul"            | 33   |
      | "LeBron James"          | 34   |
      | "Shaquille O'Neal"      | 47   |
      | NULL                    | NULL |
      | "Klay Thompson"         | 29   |
      | "Dejounte Murray"       | 29   |
      | "Vince Carter"          | 42   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Kyle Anderson"         | 25   |
      | "Blake Griffin"         | 30   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Tiago Splitter"        | 34   |
      | NULL                    | NULL |
      | "Rudy Gay"              | 32   |
      | "David West"            | 38   |
      | "Steve Nash"            | 45   |
      | "Dwyane Wade"           | 37   |
      | "Manu Ginobili"         | 41   |
      | "Jonathon Simmons"      | 29   |
      | NULL                    | NULL |
      | "Danny Green"           | 31   |
      | NULL                    | NULL |
      | "Rajon Rondo"           | 33   |
      | NULL                    | NULL |
      | "Russell Westbrook"     | 30   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Tony Parker"           | 36   |
      | NULL                    | -2   |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | NULL                    | NULL |
      | "Carmelo Anthony"       | 34   |
      | "Damian Lillard"        | 28   |
      | NULL                    | -4   |
      | "Marco Belinelli"       | 32   |
      | NULL                    | NULL |
      | "Amar'e Stoudemire"     | 36   |
      | "Paul George"           | 28   |
      | "Luka Doncic"           | 20   |
      | "Tim Duncan"            | 42   |
      | "Ricky Rubio"           | 28   |
      | NULL                    | NULL |
      | "Ben Simmons"           | 22   |
      | "Stephen Curry"         | 31   |
      | NULL                    | NULL |
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
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
      WHERE id(x) == hash('James Harden')
      RETURN v.player.name AS Name
      """
    Then a SemanticError should be raised at runtime: Alias used but not defined: `x'
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), v.player.name]
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name           |
      | "James Harden" |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
            OR v.player.age == 23
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name                 |
      | "Jonathon Simmons"   |
      | "Dejounte Murray"    |
      | "Klay Thompson"      |
      | "James Harden"       |
      | "Kristaps Porzingis" |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == hash('James Harden')
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
      WHERE id(v) == hash('James Harden')
            OR v.player.age != 23
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name                    |
      | "Stephen Curry"         |
      | NULL                    |
      | "Kyrie Irving"          |
      | "James Harden"          |
      | "Grant Hill"            |
      | "Tracy McGrady"         |
      | "Ben Simmons"           |
      | "Ricky Rubio"           |
      | "Tim Duncan"            |
      | "Joel Embiid"           |
      | "Marc Gasol"            |
      | "Cory Joseph"           |
      | "Giannis Antetokounmpo" |
      | "Aron Baynes"           |
      | "DeAndre Jordan"        |
      | "Luka Doncic"           |
      | "JaVale McGee"          |
      | "Paul George"           |
      | "Nobody"                |
      | "Kevin Durant"          |
      | "Jason Kidd"            |
      | "Amar'e Stoudemire"     |
      | "Paul Gasol"            |
      | "Marco Belinelli"       |
      | NULL                    |
      | "Dirk Nowitzki"         |
      | "LaMarcus Aldridge"     |
      | "Kobe Bryant"           |
      | "Damian Lillard"        |
      | NULL                    |
      | "Boris Diaw"            |
      | "Ray Allen"             |
      | "Dwight Howard"         |
      | "Carmelo Anthony"       |
      | "Yao Ming"              |
      | "Chris Paul"            |
      | "LeBron James"          |
      | "Shaquille O'Neal"      |
      | NULL                    |
      | "Klay Thompson"         |
      | "Dejounte Murray"       |
      | "Vince Carter"          |
      | "Tony Parker"           |
      | "Russell Westbrook"     |
      | "Rajon Rondo"           |
      | "Kyle Anderson"         |
      | "Blake Griffin"         |
      | "Danny Green"           |
      | "Jonathon Simmons"      |
      | "Tiago Splitter"        |
      | "Manu Ginobili"         |
      | "Rudy Gay"              |
      | "David West"            |
      | "Steve Nash"            |
      | "Dwyane Wade"           |

  Scenario: test OR logic
    When executing query:
      """
      MATCH (v:player)
      WHERE v.player.name == "Tim Duncan"
            OR v.player.age == 23
      RETURN v.player.name as name
      """
    Then the result should be, in any order:
      | name                 |
      | "Kristaps Porzingis" |
      | "Tim Duncan"         |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.player.name == "Tim Duncan"
            OR v.noexist.age == 23
      RETURN v.player.name as name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.player.noexist == "Tim Duncan"
            OR v.player.age == 23
      RETURN v.player.name as name
      """
    Then the result should be, in any order:
      | name                 |
      | "Kristaps Porzingis" |
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
      RETURN v.player.name as name
      """
    Then the result should be, in any order:
      | name         |
      | "Tim Duncan" |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
            OR id(v) == hash('Yao Ming')
      RETURN v.player.name as name
      """
    Then the result should be, in any order:
      | name               |
      | "James Harden"     |
      | "Jonathon Simmons" |
      | "Klay Thompson"    |
      | "Dejounte Murray"  |
      | "Yao Ming"         |

  Scenario: with arithmetic
    When executing query:
      """
      MATCH (v)
      WHERE (id(v) + 1) == hash('James Harden')
      RETURN v.player.name AS Name
      """
    Then the execution should be successful

  Scenario: Start from end
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE id(t) == hash('Pistons')
      RETURN v.player.name AS Name
      """
    Then the result should be, in any order:
      | Name            |
      | 'Aron Baynes'   |
      | 'Blake Griffin' |
      | 'Grant Hill'    |

  Scenario: [v2ga bug] Negative start vid
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1        |
      | replica_factor | 1        |
      | vid_type       | int64    |
      | charset        | utf8     |
      | collate        | utf8_bin |
    And having executed:
      """
      CREATE TAG player(name string, age int);
      """
    When executing query:
      """
      CREATE TAG INDEX player_name_index ON player(name(10));
      """
    Then the execution should be successful
    And wait 6 seconds
    When try to execute query:
      """
      INSERT VERTEX player(name, age) VALUES -100:("Tim", 32);
      """
    Then the execution should be successful
    When executing query:
      """
      MATCH (v) WHERE id(v) == -100 RETURN v;
      """
    Then the result should be, in any order:
      | v                                    |
      | (-100 :player{age: 32, name: "Tim"}) |
    When executing query:
      """
      MATCH (v) WHERE id(v) IN [-100] RETURN v;
      """
    Then the result should be, in any order:
      | v                                    |
      | (-100 :player{age: 32, name: "Tim"}) |

  Scenario: check plan
    When profiling query:
      """
      MATCH (v)
      WHERE id(v) == hash('Paul Gasol')
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
      WHERE id(v) IN [hash('Paul Gasol')]
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

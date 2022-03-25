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
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
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
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: test OR logic
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
            OR v.player.age == 23
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == hash('James Harden')
            OR v.player.age == 23
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == hash('James Harden')
            OR v.player.age != 23
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
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
      | partition_num  | 9        |
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

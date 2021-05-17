Feature: Match seek by id

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: basic
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == hash('Paul Gasol')
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
      RETURN v.name AS Name
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
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    When executing query:
      """
      MATCH (v)
      WHERE NOT NOT id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
      RETURN v.name AS Name
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
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) AND id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray'), hash('Paul Gasol')]
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | 'Paul Gasol' |

  Scenario: basic logical or
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) OR id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
      RETURN v.name AS Name
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
      RETURN v.name AS Name
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
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | 'Paul Gasol' |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == hash('Paul Gasol')) AND id(v) != hash('Paul Gasol')
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray'), hash('Paul Gasol')]
            OR false
      RETURN v.name AS Name
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
            AND (id(v) == hash('James Harden') OR v.age == 23)
      RETURN v.name AS Name
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
            AND v.name != 'Jonathon Simmons'
            AND v.age == 29
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name              |
      | 'James Harden'    |
      | 'Klay Thompson'   |
      | 'Dejounte Murray' |
    When executing query:
      """
      MATCH (v)
      WHERE (id(v) == hash("Tim Duncan") AND v.age>10) OR (id(v) == hash("Tony Parker") AND v.age>10)
      RETURN v.name AS Name
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
      RETURN v.name AS Name, t.name AS Team
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
      RETURN v.name AS Name, t.name AS Team
      """
    Then the result should be, in any order:
      | Name         | Team        |
      | 'Paul Gasol' | 'Grizzlies' |
      | 'Paul Gasol' | 'Lakers'    |
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE ((NOT NOT id(v) == hash('Paul Gasol')) AND id(v) == hash('Paul Gasol')) OR id(t) IN [hash('Grizzlies'), hash('Lakers')]
      RETURN v.name AS Name, t.name AS Team
      """
    Then the result should be, in any order:
      | Name         | Team        |
      | 'Paul Gasol' | 'Grizzlies' |
      | 'Paul Gasol' | 'Lakers'    |
      | 'Paul Gasol' | 'Bulls'     |
      | 'Paul Gasol' | 'Spurs'     |
      | 'Paul Gasol' | 'Bucks'     |

  Scenario: can't refer
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) == hash('Paul Gasol')
      RETURN v.name AS Name, v.age AS Age
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN [hash('James Harden'), hash('Jonathon Simmons'), hash('Klay Thompson'), hash('Dejounte Murray')]
            OR v.age == 23
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == hash('James Harden')
            OR v.age == 23
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE id(x) == hash('James Harden')
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:

  Scenario: with arithmetic
    When executing query:
      """
      MATCH (v)
      WHERE (id(v) + 1) == hash('James Harden')
      RETURN v.name AS Name
      """
    Then the execution should be successful

  Scenario: Start from end
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE id(t) == hash('Pistons')
      RETURN v.name AS Name
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
    When try to execute query:
      """
      INSERT VERTEX player(name, age) VALUES -100:("Tim", 32);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX player_name_index ON player(name(10));
      """
    Then the execution should be successful
    And wait 6 seconds
    When submit a job:
      """
      REBUILD TAG INDEX player_name_index;
      """
    Then wait the job to finish
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

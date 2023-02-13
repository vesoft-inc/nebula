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
    # start vid finder don't support variable currently
    When executing query:
      """
      WITH [1, 2, 3] AS coll
      UNWIND coll AS vid
      MATCH (v) WHERE id(v) == vid
      RETURN v;
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      WITH [1, 2, 3] AS coll
      UNWIND coll AS vid
      MATCH (v) WHERE id(v) == "Tony Parker" OR id(v) == vid
      RETURN v;
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

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
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
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
      MATCH (v)
      WHERE NOT id(v) == 'Paul Gasol'
      RETURN v.player.name AS Name, v.player.age AS Age
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
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
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', v.player.name]
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v) WHERE id(v) == "Tim Duncan" OR id(v) != "Tony Parker" RETURN COUNT(*) AS count
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

  Scenario: test OR logic
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
            OR v.player.age == 23
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == 'James Harden'
            OR v.player.age == 23
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == 'James Harden'
            OR v.player.age != 23
      RETURN v.player.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
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
      | 10 | Traverse       | 2            |               |
      | 2  | Dedup          | 4            |               |
      | 1  | PassThrough    | 3            |               |
      | 3  | Start          |              |               |

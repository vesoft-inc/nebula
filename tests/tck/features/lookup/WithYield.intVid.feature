Feature: Lookup with yield in integer vid

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: [1] tag with yield
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name
      """
    Then the result should be, in any order:
      | player.name     |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name, player.age + 1
      """
    Then the result should be, in any order:
      | player.name     | (player.age+1) |
      | 'Kobe Bryant'   | 41             |
      | 'Dirk Nowitzki' | 41             |
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name, player.age + 1, vertex as node
      """
    Then the result should be, in any order:
      | player.name     | (player.age+1) | node                                                          |
      | 'Kobe Bryant'   | 41             | ("Kobe Bryant" : player {age : 40, name : "Kobe Bryant"})     |
      | 'Dirk Nowitzki' | 41             | ("Dirk Nowitzki" : player {age : 40, name : "Dirk Nowitzki"}) |

  Scenario: [1] tag with yield rename
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name
      """
    Then the result should be, in any order:
      | name            |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |
    When executing query:
      """
      LOOKUP ON team WHERE team.name in ["76ers", "Lakers", "Spurs"] YIELD vertex AS node
      """
    Then the result should be, in any order:
      | node                                |
      | ("76ers" : team {name : "76ers"})   |
      | ("Lakers" : team {name : "Lakers"}) |
      | ("Spurs" : team {name : "Spurs"})   |

  Scenario: [2] edge with yield
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
      | 2008             |
      | 2008             |
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year, edge as relationship
      """
    Then the result should be, in any order:
      | serve.start_year | relationship                                                                   |
      | 2008             | [:serve "Russell Westbrook"->"Thunders" @0 {end_year: 2019, start_year: 2008}] |
      | 2008             | [:serve "Marc Gasol"->"Grizzlies" @0 {end_year: 2019, start_year: 2008}]       |

  Scenario: [2] edge with yield rename
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | startYear |
      | 2008      |
      | 2008      |
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness < 50 + 1 YIELD like.likeness, edge as relationship
      """
    Then the result should be, in any order:
      | like.likeness | relationship                                               |
      | -1            | [:like "Blake Griffin"->"Chris Paul" @0 {likeness: -1}]    |
      | 10            | [:like "Dirk Nowitzki"->"Dwyane Wade" @0 {likeness: 10}]   |
      | 13            | [:like "Kyrie Irving"->"LeBron James" @0 {likeness: 13}]   |
      | 50            | [:like "Marco Belinelli"->"Tony Parker" @0 {likeness: 50}] |
      | -1            | [:like "Rajon Rondo"->"Ray Allen" @0 {likeness: -1}]       |
      | 9             | [:like "Ray Allen"->"Rajon Rondo" @0 {likeness: 9}]        |
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear | YIELD count(*) as nums
      """
    Then the result should be, in any order:
      | nums |
      | 2    |

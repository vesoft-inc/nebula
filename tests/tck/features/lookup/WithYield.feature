Feature: Lookup with yield

  Background:
    Given a graph with space named "nba"

  Scenario: [1] tag with yield
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name
      """
    Then the result should be, in any order:
      | VertexID        | player.name     |
      | 'Kobe Bryant'   | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' |
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name, player.age + 1
      """
    Then the result should be, in any order:
      | VertexID        | player.name     | (player.age+1) |
      | 'Kobe Bryant'   | 'Kobe Bryant'   | 41             |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' | 41             |
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name, player.age + 1, vertex as node
      """
    Then the result should be, in any order:
      | VertexID        | player.name     | (player.age+1) | node                                                          |
      | 'Kobe Bryant'   | 'Kobe Bryant'   | 41             | ("Kobe Bryant" : player {age : 40, name : "Kobe Bryant"})     |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' | 41             | ("Dirk Nowitzki" : player {age : 40, name : "Dirk Nowitzki"}) |

  Scenario: [1] tag with yield rename
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name
      """
    Then the result should be, in any order:
      | VertexID        | name            |
      | 'Kobe Bryant'   | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' |
    When executing query:
      """
      LOOKUP ON team WHERE team.name in ["76ers", "Lakers", "Spurs"] YIELD vertex AS node
      """
    Then the result should be, in any order:
      | VertexID | node                                |
      | '76ers'  | ("76ers" : team {name : "76ers"})   |
      | 'Lakers' | ("Lakers" : team {name : "Lakers"}) |
      | 'Spurs'  | ("Spurs" : team {name : "Spurs"})   |

  Scenario: [2] edge with yield
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID      | Ranking | serve.start_year |
      | 'Russell Westbrook' | 'Thunders'  | 0       | 2008             |
      | 'Marc Gasol'        | 'Grizzlies' | 0       | 2008             |
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year, edge as relationship
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID      | Ranking | serve.start_year | relationship                                                                   |
      | 'Russell Westbrook' | 'Thunders'  | 0       | 2008             | [:serve "Russell Westbrook"->"Thunders" @0 {end_year: 2019, start_year: 2008}] |
      | 'Marc Gasol'        | 'Grizzlies' | 0       | 2008             | [:serve "Marc Gasol"->"Grizzlies" @0 {end_year: 2019, start_year: 2008}]       |

  Scenario: [2] edge with yield rename
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID      | Ranking | startYear |
      | 'Russell Westbrook' | 'Thunders'  | 0       | 2008      |
      | 'Marc Gasol'        | 'Grizzlies' | 0       | 2008      |
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness < 50 + 1 YIELD like.likeness, edge as relationship
      """
    Then the result should be, in any order:
      | SrcVID            | DstVID         | Ranking | like.likeness | relationship                                               |
      | "Blake Griffin"   | "Chris Paul"   | 0       | -1            | [:like "Blake Griffin"->"Chris Paul" @0 {likeness: -1}]    |
      | "Dirk Nowitzki"   | "Dwyane Wade"  | 0       | 10            | [:like "Dirk Nowitzki"->"Dwyane Wade" @0 {likeness: 10}]   |
      | "Kyrie Irving"    | "LeBron James" | 0       | 13            | [:like "Kyrie Irving"->"LeBron James" @0 {likeness: 13}]   |
      | "Marco Belinelli" | "Tony Parker"  | 0       | 50            | [:like "Marco Belinelli"->"Tony Parker" @0 {likeness: 50}] |
      | "Rajon Rondo"     | "Ray Allen"    | 0       | -1            | [:like "Rajon Rondo"->"Ray Allen" @0 {likeness: -1}]       |
      | "Ray Allen"       | "Rajon Rondo"  | 0       | 9             | [:like "Ray Allen"->"Rajon Rondo" @0 {likeness: 9}]        |

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

  Scenario: [1] tag with yield rename
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name
      """
    Then the result should be, in any order:
      | VertexID        | name            |
      | 'Kobe Bryant'   | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' |

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

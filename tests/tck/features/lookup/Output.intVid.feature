Feature: Lookup with output in integer vid

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: [1] tag output
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 |
      FETCH PROP ON player $-.VertexID YIELD player.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID        | player.name     |
      | 'Kobe Bryant'   | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' |

  Scenario: [1] tag ouput with yield rename
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name |
      FETCH PROP ON player $-.VertexID YIELD player.name AS name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID        | name            |
      | 'Kobe Bryant'   | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' |

  Scenario: [1] tag output by var
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.age == 40;
      FETCH PROP ON player $a.VertexID YIELD player.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID        | player.name     |
      | 'Kobe Bryant'   | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' |

  Scenario: [1] tag ouput with yield rename by var
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name;
      FETCH PROP ON player $a.VertexID YIELD player.name AS name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID        | name            |
      | 'Kobe Bryant'   | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' | 'Dirk Nowitzki' |

  Scenario: [2] edge output
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year |
      FETCH PROP ON serve $-.SrcVID->$-.DstVID YIELD serve.start_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src          | serve._dst  | serve._rank | serve.start_year |
      | 'Russell Westbrook' | 'Thunders'  | 0           | 2008             |
      | 'Marc Gasol'        | 'Grizzlies' | 0           | 2008             |

  Scenario: [2] edge output with yield rename
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear |
      FETCH PROP ON serve $-.SrcVID->$-.DstVID YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src          | serve._dst  | serve._rank | startYear |
      | 'Russell Westbrook' | 'Thunders'  | 0           | 2008      |
      | 'Marc Gasol'        | 'Grizzlies' | 0           | 2008      |

  Scenario: [2] edge output by var
    When executing query:
      """
      $a = LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year;
      FETCH PROP ON serve $a.SrcVID->$a.DstVID YIELD serve.start_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src          | serve._dst  | serve._rank | serve.start_year |
      | 'Russell Westbrook' | 'Thunders'  | 0           | 2008             |
      | 'Marc Gasol'        | 'Grizzlies' | 0           | 2008             |

  Scenario: [2] edge output with yield rename by var
    When executing query:
      """
      $a = LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear;
      FETCH PROP ON serve $a.SrcVID->$a.DstVID YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src          | serve._dst  | serve._rank | startYear |
      | 'Russell Westbrook' | 'Thunders'  | 0           | 2008      |
      | 'Marc Gasol'        | 'Grizzlies' | 0           | 2008      |

Feature: Lookup with output

  Background:
    Given a graph with space named "nba"

  Scenario: [1] tag output
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 |
      FETCH PROP ON player $-.VertexID YIELD player.name
      """
    Then the result should be, in any order:
      | player.name     |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |

  Scenario: [1] tag ouput with yield rename
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name |
      FETCH PROP ON player $-.name YIELD player.name AS name
      """
    Then the result should be, in any order:
      | name            |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |

  Scenario: [1] tag output by var
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.age == 40;
      FETCH PROP ON player $a.VertexID YIELD player.name
      """
    Then the result should be, in any order:
      | player.name     |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |

  Scenario: [1] tag ouput with yield rename by var
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name;
      FETCH PROP ON player $a.name YIELD player.name AS name
      """
    Then the result should be, in any order:
      | name            |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |

  Scenario: [2] edge output
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year |
      FETCH PROP ON serve $-.SrcVID->$-.DstVID YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
      | 2008             |
      | 2008             |

  Scenario: [2] edge output with yield rename
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear |
      FETCH PROP ON serve $-.SrcVID->$-.DstVID YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | startYear |
      | 2008      |
      | 2008      |

  Scenario: [2] edge output by var
    When executing query:
      """
      $a = LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year;
      FETCH PROP ON serve $a.SrcVID->$a.DstVID YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
      | 2008             |
      | 2008             |

  Scenario: [2] edge output with yield rename by var
    When executing query:
      """
      $a = LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear;
      FETCH PROP ON serve $a.SrcVID->$a.DstVID YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | startYear |
      | 2008      |
      | 2008      |

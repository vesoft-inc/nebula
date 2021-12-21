Feature: Lookup with output in integer vid

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: [1] tag output
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD id(vertex) as id |
      FETCH PROP ON player $-.id YIELD player.name
      """
    Then the result should be, in any order:
      | player.name     |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |

  Scenario: [1] tag output with yield rename
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name, id(vertex) as id |
      FETCH PROP ON player $-.id YIELD player.name AS name
      """
    Then the result should be, in any order:
      | name            |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |

  Scenario: [1] tag output by var
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.age == 40 YIELD id(vertex) as id;
      FETCH PROP ON player $a.id YIELD player.name
      """
    Then the result should be, in any order:
      | player.name     |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |

  Scenario: [1] tag output with yield rename by var
    When executing query:
      """
      $a = LOOKUP ON player WHERE player.age == 40 YIELD id(vertex) as id, player.name AS name;
      FETCH PROP ON player $a.id YIELD player.name AS name
      """
    Then the result should be, in any order:
      | name            |
      | 'Kobe Bryant'   |
      | 'Dirk Nowitzki' |

  Scenario: [2] edge output
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD src(edge) as src, dst(edge) as dst, serve.start_year |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
      | 2008             |
      | 2008             |

  Scenario: [2] edge output with yield rename
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear, src(edge) as src, dst(edge) as dst |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | startYear |
      | 2008      |
      | 2008      |

  Scenario: [2] edge output by var
    When executing query:
      """
      $a = LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year, src(edge) as src, dst(edge) as dst;
      FETCH PROP ON serve $a.src->$a.dst YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
      | 2008             |
      | 2008             |

  Scenario: [2] edge output with yield rename by var
    When executing query:
      """
      $a = LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear, src(edge) as src, dst(edge) as dst;
      FETCH PROP ON serve $a.src->$a.dst YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | startYear |
      | 2008      |
      | 2008      |

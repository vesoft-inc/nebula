Feature: Fetch Int Vid Edges

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: [1] Base fetch prop on an edge and return the specific properties
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw') -> hash('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | 2003             | 2005           |

  Scenario: [2] Fetch prop on an edge
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks') YIELD serve.start_year > 2001, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | (serve.start_year>2001) | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | True                    | 2005           |

  Scenario: [3] Fetch prop on the edgetype of a edge with ranking
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks')@0 YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | 2003             | 2005           |

  Scenario: [4] Fetch prop on multiple edges
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks'),hash('Boris Diaw')->hash('Suns') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | 2003             | 2005           |
      | hash("Boris Diaw") | hash("Suns")  | 0           | 2005             | 2008           |

  Scenario: [5] Fetch prop works with pipeline
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst      | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Suns")    | 0           | 2005             | 2008           |
      | hash("Boris Diaw") | hash("Hawks")   | 0           | 2003             | 2005           |
      | hash("Boris Diaw") | hash("Spurs")   | 0           | 2012             | 2016           |
      | hash("Boris Diaw") | hash("Hornets") | 0           | 2008             | 2012           |
      | hash("Boris Diaw") | hash("Jazz")    | 0           | 2016             | 2017           |

  Scenario: [6] Fetch prop works with user define variable
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst; FETCH PROP ON serve $var.src->$var.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst      | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Suns")    | 0           | 2005             | 2008           |
      | hash("Boris Diaw") | hash("Hawks")   | 0           | 2003             | 2005           |
      | hash("Boris Diaw") | hash("Spurs")   | 0           | 2012             | 2016           |
      | hash("Boris Diaw") | hash("Hornets") | 0           | 2008             | 2012           |
      | hash("Boris Diaw") | hash("Jazz")    | 0           | 2016             | 2017           |

  Scenario: [7] Fetch prop works with hash function
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | 2003             | 2005           |

  @skip
  Scenario: [8] Fetch prop works with uuid
    When executing query:
      """
      FETCH PROP ON serve uuid('Boris Diaw')->uuid('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src           | serve._dst          | serve._rank | serve.start_year | serve.end_year |
      | -7391649757168799460 | 3973677956883673372 | 0           | 2003             | 2005           |

  Scenario: [9] Fetch prop on an edge without yield
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks')
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | 2003             | 2005           |

  Scenario: [10] Fetch prop on a edge with a rank,but without yield
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks')@0
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | 2003             | 2005           |

  Scenario: [11] Fetch prop on a edge
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks')
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | 2003             | 2005           |

  @skip
  Scenario: [12] Fetch prop works with uuid, but without YIELD
    When executing query:
      """
      FETCH PROP ON serve uuid('Boris Diaw')->uuid('Hawks')
      """
    Then the result should be, in any order:
      | serve._src           | serve._dst          | serve._rank | serve.start_year | serve.end_year |
      | -7391649757168799460 | 3973677956883673372 | 0           | 2003             | 2005           |

  Scenario: [13] Fetch prop works with DISTINCT
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks'),hash('Boris Diaw')->hash('Hawks') YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Hawks") | 0           | 2003             | 2005           |

  Scenario: [14] Fetch prop works with DISTINCT and pipeline
    When executing query:
      """
      GO FROM hash('Boris Diaw'),hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst | FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst      | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Suns")    | 0           | 2005             | 2008           |
      | hash("Boris Diaw") | hash("Hawks")   | 0           | 2003             | 2005           |
      | hash("Boris Diaw") | hash("Spurs")   | 0           | 2012             | 2016           |
      | hash("Boris Diaw") | hash("Hornets") | 0           | 2008             | 2012           |
      | hash("Boris Diaw") | hash("Jazz")    | 0           | 2016             | 2017           |

  Scenario: [15] Fetch prop works with DISTINCT and user define variable
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw'),hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst; FETCH PROP ON serve $var.src->$var.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst      | serve._rank | serve.start_year | serve.end_year |
      | hash("Boris Diaw") | hash("Suns")    | 0           | 2005             | 2008           |
      | hash("Boris Diaw") | hash("Hawks")   | 0           | 2003             | 2005           |
      | hash("Boris Diaw") | hash("Spurs")   | 0           | 2012             | 2016           |
      | hash("Boris Diaw") | hash("Hornets") | 0           | 2008             | 2012           |
      | hash("Boris Diaw") | hash("Jazz")    | 0           | 2016             | 2017           |

  Scenario: [16] Fetch prop works with DISTINCT
    When executing query:
      """
      GO FROM hash('Tim Duncan'),hash('Tony Parker') OVER serve YIELD serve._src AS src, serve._dst AS dst | FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve._dst as dst
      """
    Then the result should be, in any order:
      | serve._src          | serve._dst      | serve._rank | dst             |
      | hash('Tim Duncan')  | hash('Spurs')   | 0           | hash('Spurs')   |
      | hash('Tony Parker') | hash('Spurs')   | 0           | hash('Spurs')   |
      | hash('Tony Parker') | hash('Hornets') | 0           | hash('Hornets') |

  Scenario: [17] Fetch prop on not existing vertex
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve._src AS src, serve._dst AS dst | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |

  Scenario: [18] Fetch prop on not existing vertex
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start | YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000 | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |

  Scenario: [19] Fetch prop Semantic Error
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD $^.serve.start_year
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [20] Fetch prop Semantic Error
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD $$.serve.start_year
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [21] Fetch prop Semantic Error
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD abc.start_year
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [22] Fetch prop on not exist edge
    When executing query:
      """
      FETCH PROP ON serve hash("Zion Williamson")->hash("Spurs") YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year |

  @skip
  Scenario: [23] Fetch prop Semantic Error
    When executing query:
      """
      FETCH PROP ON serve uuid("Zion Williamson")->uuid("Spurs") YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year |

  Scenario: [24] Fetch prop on a edge and return duplicate columns
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash("Spurs") YIELD serve.start_year, serve.start_year
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve.start_year | serve.start_year |
      | hash("Boris Diaw") | hash("Spurs") | 0           | 2012             | 2012             |

  Scenario: [25] Fetch prop on an edge and return duplicate _src, _dst and _rank
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash("Spurs") YIELD serve._src, serve._dst, serve._rank
      """
    Then the result should be, in any order:
      | serve._src         | serve._dst    | serve._rank | serve._src         | serve._dst    | serve._rank |
      | hash("Boris Diaw") | hash("Spurs") | 0           | hash("Boris Diaw") | hash("Spurs") | 0           |

  Scenario: [26] Fetch prop on illegal input
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS src | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [27] Fetch prop returns not existing property
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks') YIELD serve.start_year1
      """
    Then a SemanticError should be raised at runtime:

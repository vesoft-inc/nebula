Feature: Fetch String Vid Edges

  Background:
    Given a graph with space named "nba"

  Scenario: Base fetch prop on an edge
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw' -> 'Hawks' YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs"
      """
    Then the result should be, in any order:
      | edges_                                                               |
      | [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}] |
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Not Exist"
      """
    Then the result should be, in any order:
      | edges_ |

  Scenario: Fetch prop on an edge
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks' YIELD serve.start_year > 2001, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | (serve.start_year>2001) | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | True                    | 2005           |
    # Fetch prop on the edgetype of a edge with ranking
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks'@0 YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
    # Fetch prop on an edge without yield
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks' YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
    # Fetch prop on a edge with a rank,but without yield
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks'@0 YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |

  Scenario: Fetch prop on multiple edges
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks','Boris Diaw'->'Suns' YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |

  Scenario: Fetch prop works with pipeline
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2016           |
      | "Boris Diaw" | "Hornets"  | 0           | 2008             | 2012           |
      | "Boris Diaw" | "Jazz"     | 0           | 2016             | 2017           |

  Scenario: Fetch prop works with user define variable
    When executing query:
      """
      $var = GO FROM 'Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst; FETCH PROP ON serve $var.src->$var.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2016           |
      | "Boris Diaw" | "Hornets"  | 0           | 2008             | 2012           |
      | "Boris Diaw" | "Jazz"     | 0           | 2016             | 2017           |

  Scenario: Fetch prop works with DISTINCT
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks','Boris Diaw'->'Hawks' YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
    # Fetch prop works with DISTINCT and pipeline
    When executing query:
      """
      GO FROM 'Boris Diaw','Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst | FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2016           |
      | "Boris Diaw" | "Hornets"  | 0           | 2008             | 2012           |
      | "Boris Diaw" | "Jazz"     | 0           | 2016             | 2017           |
    When executing query:
      """
      GO FROM 'Tim Duncan','Tony Parker' OVER serve YIELD serve._src AS src, serve._dst AS dst | FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve._dst as dst
      """
    Then the result should be, in any order:
      | serve._src    | serve._dst | serve._rank | dst       |
      | 'Tim Duncan'  | 'Spurs'    | 0           | 'Spurs'   |
      | 'Tony Parker' | 'Spurs'    | 0           | 'Spurs'   |
      | 'Tony Parker' | 'Hornets'  | 0           | 'Hornets' |
    # Fetch prop works with DISTINCT and user define variable
    When executing query:
      """
      $var = GO FROM 'Boris Diaw','Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst; FETCH PROP ON serve $var.src->$var.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2016           |
      | "Boris Diaw" | "Hornets"  | 0           | 2008             | 2012           |
      | "Boris Diaw" | "Jazz"     | 0           | 2016             | 2017           |

  Scenario: Fetch prop on not existing vertex
    When executing query:
      """
      FETCH PROP ON serve "Zion Williamson"->"Spurs" YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year |
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve._src AS src, serve._dst AS dst
      | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start
      | YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000
      | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start
                   | YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000
                   | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |

  Scenario: Fetch prop on exist and not exist edges
    When executing query:
      """
      FETCH PROP ON serve "Zion Williamson"->"Spurs", "Boris Diaw"->"Hawks" YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             |

  Scenario: Fetch prop on a edge and return duplicate columns
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD serve.start_year, serve.start_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.start_year |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2012             |

  Scenario: Fetch prop on an edge and return duplicate _src, _dst and _rank
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->"Spurs" YIELD serve._src, serve._dst, serve._rank
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve._src   | serve._dst | serve._rank |
      | "Boris Diaw" | "Spurs"    | 0           | "Boris Diaw" | "Spurs"    | 0           |

  Scenario: Fetch and Yield
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" | yield properties($-.edges_)
      """
    Then the result should be, in any order:
      | properties($-.edges_) |
      | {likeness: 95}        |
      | {likeness: 90}        |
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" | yield startNode($-.edges_) AS nodes
      """
    Then the result should be, in any order, with relax comparison:
      | nodes           |
      | ("Tony Parker") |
      | ("Grant Hill")  |
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" | yield endNode($-.edges_) AS nodes
      """
    Then the result should be, in any order, with relax comparison:
      | nodes             |
      | ("Tim Duncan")    |
      | ("Tracy McGrady") |

  Scenario: Fetch prop Semantic Error
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD $^.serve.start_year
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD $$.serve.start_year
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD abc.start_year
      """
    Then a SemanticError should be raised at runtime:
    # Fetch prop on illegal input
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS src
      | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then a SemanticError should be raised at runtime:
    # Fetch prop returns not existing property
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks' YIELD serve.not_exist_prop
      """
    Then a SemanticError should be raised at runtime:

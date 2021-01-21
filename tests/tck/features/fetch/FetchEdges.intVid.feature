Feature: Fetch Int Vid Edges

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Base fetch prop on an edge
    # return the specific properties
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw') -> hash('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks') YIELD serve.start_year > 2001, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | (serve.start_year>2001) | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | True                    | 2005           |
    # Fetch prop on an edge without yield
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs")
      """
    Then the result should be, in any order:
      | edges_                                                                           |
      | [:serve hash("Boris Diaw")->hash("Spurs") @0 {end_year: 2016, start_year: 2012}] |
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Not Exist")
      """
    Then the result should be, in any order:
      | edges_ |
    # Fetch prop on the edgetype of a edge with ranking
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks')@0 YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
    # Fetch prop on a edge with a rank,but without yield
    When executing query:
      """
      FETCH PROP ON like hash("Tony Parker")->hash("Tim Duncan")@0
      """
    Then the result should be, in any order:
      | edges_                                                            |
      | [:like hash("Tony Parker")->hash("Tim Duncan") @0 {likeness: 95}] |

  Scenario: Fetch prop on multiple edges
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks'),hash('Boris Diaw')->hash('Suns') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
    # fetch prop on exist and not exist edges
    When executing query:
      """
      FETCH PROP ON serve hash("Zion Williamson")->hash("Spurs"), hash("Boris Diaw")->hash("Hawks") YIELD serve.start_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             |

  Scenario: Fetch prop works with pipeline
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst
      | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2016           |
      | "Boris Diaw" | "Hornets"  | 0           | 2008             | 2012           |
      | "Boris Diaw" | "Jazz"     | 0           | 2016             | 2017           |

  Scenario: Fetch prop works with user define variable
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst;
      FETCH PROP ON serve $var.src->$var.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2016           |
      | "Boris Diaw" | "Hornets"  | 0           | 2008             | 2012           |
      | "Boris Diaw" | "Jazz"     | 0           | 2016             | 2017           |

  @skip
  Scenario: Fetch prop works with uuid
    When executing query:
      """
      FETCH PROP ON serve uuid('Boris Diaw')->uuid('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src           | serve._dst          | serve._rank | serve.start_year | serve.end_year |
      | -7391649757168799460 | 3973677956883673372 | 0           | 2003             | 2005           |
    # Fetch prop works with uuid, but without YIELD
    When executing query:
      """
      FETCH PROP ON serve uuid('Boris Diaw')->uuid('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src           | serve._dst          | serve._rank | serve.start_year | serve.end_year |
      | -7391649757168799460 | 3973677956883673372 | 0           | 2003             | 2005           |
    # Fetch prop works with not existing edge
    When executing query:
      """
      FETCH PROP ON serve uuid("Zion Williamson")->uuid("Spurs") YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year |

  Scenario: Fetch prop works with DISTINCT
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks'),hash('Boris Diaw')->hash('Hawks') YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
    # Fetch prop works with DISTINCT and pipeline
    When executing query:
      """
      GO FROM hash('Boris Diaw'),hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst
      | FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2016           |
      | "Boris Diaw" | "Hornets"  | 0           | 2008             | 2012           |
      | "Boris Diaw" | "Jazz"     | 0           | 2016             | 2017           |
    When executing query:
      """
      GO FROM hash('Tim Duncan'),hash('Tony Parker') OVER serve YIELD serve._src AS src, serve._dst AS dst
      | FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve._dst as dst
      """
    Then the result should be, in any order, and the columns 0,1,3 should be hashed:
      | serve._src    | serve._dst | serve._rank | dst       |
      | 'Tim Duncan'  | 'Spurs'    | 0           | 'Spurs'   |
      | 'Tony Parker' | 'Spurs'    | 0           | 'Spurs'   |
      | 'Tony Parker' | 'Hornets'  | 0           | 'Hornets' |
    # Fetch prop works with DISTINCT and user define variable
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw'),hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst;
      FETCH PROP ON serve $var.src->$var.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Boris Diaw" | "Suns"     | 0           | 2005             | 2008           |
      | "Boris Diaw" | "Hawks"    | 0           | 2003             | 2005           |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2016           |
      | "Boris Diaw" | "Hornets"  | 0           | 2008             | 2012           |
      | "Boris Diaw" | "Jazz"     | 0           | 2016             | 2017           |

  Scenario: Fetch prop on not existing edge
    # Fetch prop on not exist edge
    When executing query:
      """
      FETCH PROP ON serve hash("Zion Williamson")->hash("Spurs") YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve._src AS src, serve._dst AS dst
      | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start
      | YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000
      | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start
                   | YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000
                   | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |

  Scenario: Fetch prop Semantic Error
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD $^.serve.start_year
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD $$.serve.start_year
      """
    Then a SemanticError should be raised at runtime:
    # yield not existing edgetype
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD abc.start_year
      """
    Then a SemanticError should be raised at runtime:
    # Fetch prop returns not existing property
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks') YIELD serve.start_year1
      """
    Then a SemanticError should be raised at runtime:
    # Fetch prop on illegal input
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS src
      | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Fetch prop on a edge and return duplicate columns
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash("Spurs") YIELD serve.start_year, serve.start_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.start_year |
      | "Boris Diaw" | "Spurs"    | 0           | 2012             | 2012             |

  Scenario: Fetch prop on an edge and return duplicate _src, _dst and _rank
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash("Spurs") YIELD serve._src, serve._dst, serve._rank
      """
    Then the result should be, in any order, and the columns 0,1,3,4 should be hashed:
      | serve._src   | serve._dst | serve._rank | serve._src   | serve._dst | serve._rank |
      | "Boris Diaw" | "Spurs"    | 0           | "Boris Diaw" | "Spurs"    | 0           |

Feature: Fetch Int Vid Edges

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Base fetch prop on an edge
    # return the specific properties
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw') -> hash('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks') YIELD serve.start_year > 2001, serve.end_year
      """
    Then the result should be, in any order:
      | (serve.start_year>2001) | serve.end_year |
      | True                    | 2005           |
    # Fetch prop on an edge without yield
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                                |
      | [:serve hash("Boris Diaw")->hash("Spurs") @0 {end_year: 2016, start_year: 2012}] |
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Not Exist") YIELD edge as e
      """
    Then the result should be, in any order:
      | e |
    # Fetch prop on the edgetype of a edge with ranking
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks')@0 YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    # Fetch prop on a edge with a rank,but without yield
    When executing query:
      """
      FETCH PROP ON like hash("Tony Parker")->hash("Tim Duncan")@0 YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                 |
      | [:like hash("Tony Parker")->hash("Tim Duncan") @0 {likeness: 95}] |

  Scenario: Fetch prop on multiple edges
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks'),hash('Boris Diaw')->hash('Suns') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
      | 2005             | 2008           |
    # fetch prop on exist and not exist edges
    When executing query:
      """
      FETCH PROP ON serve hash("Zion Williamson")->hash("Spurs"), hash("Boris Diaw")->hash("Hawks") YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
      | 2003             |

  Scenario: Fetch prop works with pipeline
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2005             | 2008           |
      | 2003             | 2005           |
      | 2012             | 2016           |
      | 2008             | 2012           |
      | 2016             | 2017           |

  Scenario: Fetch prop works with user define variable
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst;
      FETCH PROP ON serve $var.src->$var.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2005             | 2008           |
      | 2003             | 2005           |
      | 2012             | 2016           |
      | 2008             | 2012           |
      | 2016             | 2017           |

  @skip
  Scenario: Fetch prop works with uuid
    When executing query:
      """
      FETCH PROP ON serve uuid('Boris Diaw')->uuid('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    # Fetch prop works with uuid, but without YIELD
    When executing query:
      """
      FETCH PROP ON serve uuid('Boris Diaw')->uuid('Hawks') YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    # Fetch prop works with not existing edge
    When executing query:
      """
      FETCH PROP ON serve uuid("Zion Williamson")->uuid("Spurs") YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |

  Scenario: Fetch prop works with DISTINCT
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks'),hash('Boris Diaw')->hash('Hawks') YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    # Fetch prop works with DISTINCT and pipeline
    When executing query:
      """
      GO FROM hash('Boris Diaw'),hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst |
      FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2005             | 2008           |
      | 2003             | 2005           |
      | 2012             | 2016           |
      | 2008             | 2012           |
      | 2016             | 2017           |
    When executing query:
      """
      GO FROM hash('Tim Duncan'),hash('Tony Parker') OVER serve YIELD serve._src AS src, serve._dst AS dst |
      FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve._dst as dst
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | dst       |
      | 'Spurs'   |
      | 'Hornets' |
    # Fetch prop works with DISTINCT and user define variable
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw'),hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst;
      FETCH PROP ON serve $var.src->$var.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order, with relax comparison:
      | serve.start_year | serve.end_year |
      | 2005             | 2008           |
      | 2003             | 2005           |
      | 2012             | 2016           |
      | 2008             | 2012           |
      | 2016             | 2017           |

  Scenario: Fetch prop on not existing edge
    # Fetch prop on not exist edge
    When executing query:
      """
      FETCH PROP ON serve hash("Zion Williamson")->hash("Spurs") YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve._src AS src, serve._dst AS dst |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start |
      YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000 |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start |
      YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000 |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |

  Scenario: Fetch prop Error
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD $^.serve.start_year
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD $$.serve.start_year
      """
    Then a ExecutionError should be raised at runtime:
    # yield not existing edgetype
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD abc.start_year
      """
    Then a ExecutionError should be raised at runtime:
    # Fetch prop returns not existing property
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash('Hawks') YIELD serve.start_year1
      """
    Then a SemanticError should be raised at runtime:
    # Fetch prop on illegal input
    When executing query:
      """
      GO FROM hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS src |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Fetch prop on a edge and return duplicate columns
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash("Spurs") YIELD serve.start_year, serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.start_year |
      | 2012             | 2012             |

  Scenario: Fetch prop on an edge and return duplicate _src, _dst and _rank
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw')->hash("Spurs") YIELD serve._src, serve._dst, serve._rank
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src   | serve._dst | serve._rank |
      | "Boris Diaw" | "Spurs"    | 0           |

  Scenario: yield edge
    When executing query:
      """
      FETCH PROP ON serve hash('Boris Diaw') -> hash('Hawks') YIELD serve.start_year, serve.end_year, edge as relationship
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year | relationship                                                                     |
      | 2003             | 2005           | [:serve hash("Boris Diaw")->hash("Hawks") @0 {end_year: 2005, start_year: 2003}] |
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Spurs") YIELD edge as relationship, src(edge) as src_edge, dst(edge) as dst_edge, type(edge) as type, rank(edge) as rank
      """
    Then the result should be, in any order, and the columns 1,2 should be hashed:
      | relationship                                                                     | src_edge     | dst_edge | type    | rank |
      | [:serve hash("Boris Diaw")->hash("Spurs") @0 {end_year: 2016, start_year: 2012}] | "Boris Diaw" | "Spurs"  | "serve" | 0    |
    When executing query:
      """
      FETCH PROP ON serve hash("Boris Diaw")->hash("Not Exist") YIELD src(edge) as a
      """
    Then the result should be, in any order, with relax comparison:
      | a |
    When executing query:
      """
      FETCH PROP ON like hash("Tony Parker")->hash("Tim Duncan"), hash("Grant Hill") -> hash("Tracy McGrady") YIELD edge as relationship |
      YIELD properties($-.relationship)
      """
    Then the result should be, in any order, with relax comparison:
      | properties($-.relationship) |
      | {likeness: 95}              |
      | {likeness: 90}              |
    When executing query:
      """
      FETCH PROP ON like hash("Tony Parker")->hash("Tim Duncan"), hash("Grant Hill") -> hash("Tracy McGrady") YIELD properties(edge) as properties
      """
    Then the result should be, in any order, with relax comparison:
      | properties     |
      | {likeness: 95} |
      | {likeness: 90} |
    When executing query:
      """
      FETCH PROP ON like hash("Tony Parker")->hash("Tim Duncan"), hash("Grant Hill") -> hash("Tracy McGrady") YIELD edge as relationship |
      YIELD startNode($-.relationship) AS node
      """
    Then the result should be, in any order, with relax comparison:
      | node            |
      | ("Tony Parker") |
      | ("Grant Hill")  |
    When executing query:
      """
      FETCH PROP ON like hash("Tony Parker")->hash("Tim Duncan"), hash("Grant Hill") -> hash("Tracy McGrady") YIELD edge as relationship |
      YIELD endNode($-.relationship) AS node
      """
    Then the result should be, in any order, with relax comparison:
      | node              |
      | ("Tim Duncan")    |
      | ("Tracy McGrady") |
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw'),hash('Boris Diaw') OVER serve YIELD serve._src AS src, serve._dst AS dst;
      FETCH PROP ON serve $var.src->$var.dst YIELD DISTINCT serve.start_year, serve.end_year, edge as relationship
      """
    Then the result should be, in any order, with relax comparison:
      | serve.start_year | serve.end_year | relationship                                                                       |
      | 2005             | 2008           | [:serve hash("Boris Diaw")->hash("Suns") @0 {end_year: 2008, start_year: 2005}]    |
      | 2003             | 2005           | [:serve hash("Boris Diaw")->hash("Hawks") @0 {end_year: 2005, start_year: 2003}]   |
      | 2012             | 2016           | [:serve hash("Boris Diaw")->hash("Spurs") @0 {end_year: 2016, start_year: 2012}]   |
      | 2008             | 2012           | [:serve hash("Boris Diaw")->hash("Hornets") @0 {end_year: 2012, start_year: 2008}] |
      | 2016             | 2017           | [:serve hash("Boris Diaw")->hash("Jazz") @0 {end_year: 2017, start_year: 2016}]    |

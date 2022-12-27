Feature: Fetch String Vid Edges

  Background:
    Given a graph with space named "nba"

  Scenario: Base fetch prop on an edge
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw' -> 'Hawks' YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                    |
      | [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}] |
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Not Exist" YIELD edge as e
      """
    Then the result should be, in any order:
      | e |

  Scenario: Fetch prop on an edge
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks' YIELD serve.start_year > 2001, serve.end_year
      """
    Then the result should be, in any order:
      | (serve.start_year>2001) | serve.end_year |
      | True                    | 2005           |
    # Fetch prop on the edgetype of a edge with ranking
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks'@0 YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    # Fetch prop on an edge without yield
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks' YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    # Fetch prop on a edge with a rank,but without yield
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks'@0 YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |

  Scenario: Fetch prop on multiple edges
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks','Boris Diaw'->'Suns' YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
      | 2005             | 2008           |

  Scenario: Fetch prop works with pipeline
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst | FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
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
      $var = GO FROM 'Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst;
      FETCH PROP ON serve $var.src->$var.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2005             | 2008           |
      | 2003             | 2005           |
      | 2012             | 2016           |
      | 2008             | 2012           |
      | 2016             | 2017           |

  Scenario: Fetch prop works with DISTINCT
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks','Boris Diaw'->'Hawks' YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2003             | 2005           |
    # Fetch prop works with DISTINCT and pipeline
    When executing query:
      """
      GO FROM 'Boris Diaw','Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst |
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
      GO FROM 'Tim Duncan','Tony Parker' OVER serve YIELD serve._src AS src, serve._dst AS dst |
      FETCH PROP ON serve $-.src->$-.dst YIELD DISTINCT serve._dst as dst
      """
    Then the result should be, in any order:
      | dst       |
      | 'Spurs'   |
      | 'Hornets' |
    # Fetch prop works with DISTINCT and user define variable
    When executing query:
      """
      $var = GO FROM 'Boris Diaw','Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst;
      FETCH PROP ON serve $var.src->$var.dst YIELD DISTINCT serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
      | 2005             | 2008           |
      | 2003             | 2005           |
      | 2012             | 2016           |
      | 2008             | 2012           |
      | 2016             | 2017           |

  Scenario: Fetch prop on not existing vertex
    When executing query:
      """
      FETCH PROP ON serve "Zion Williamson"->"Spurs" YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve._src AS src, serve._dst AS dst |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start |
      YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000 |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start |
      YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000 |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year |

  Scenario: Fetch prop on exist and not exist edges
    When executing query:
      """
      FETCH PROP ON serve "Zion Williamson"->"Spurs", "Boris Diaw"->"Hawks" YIELD serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year |
      | 2003             |

  Scenario: Fetch prop on a edge and return duplicate columns
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD serve.start_year, serve.start_year
      """
    Then the result should be, in any order:
      | serve.start_year | serve.start_year |
      | 2012             | 2012             |

  Scenario: Fetch prop on an edge and return duplicate _src, _dst and _rank
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->"Spurs" YIELD serve._src, serve._dst, serve._rank
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank |
      | "Boris Diaw" | "Spurs"    | 0           |

  Scenario: Fetch and Yield
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" YIELD edge as e |
      YIELD properties($-.e)
      """
    Then the result should be, in any order:
      | properties($-.e) |
      | {likeness: 95}   |
      | {likeness: 90}   |
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" YIELD edge as e |
      YIELD startNode($-.e) AS nodes
      """
    Then the result should be, in any order, with relax comparison:
      | nodes           |
      | ("Tony Parker") |
      | ("Grant Hill")  |
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" YIELD edge as e |
      YIELD endNode($-.e) AS nodes
      """
    Then the result should be, in any order, with relax comparison:
      | nodes             |
      | ("Tim Duncan")    |
      | ("Tracy McGrady") |

  Scenario: Fetch prop Error
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD $^.serve.start_year
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `serve`
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD $$.serve.start_year
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `serve`
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD abc.start_year
      """
    Then a ExecutionError should be raised at runtime: EdgeNotFound: EdgeName `abc`
    # Fetch prop on illegal input
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS src |
      FETCH PROP ON serve $-.src->$-.dst YIELD serve.start_year, serve.end_year
      """
    Then a SemanticError should be raised at runtime: `$-.dst', not exist prop `dst'
    # Fetch prop returns not existing property
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw'->'Hawks' YIELD serve.not_exist_prop
      """
    Then a SemanticError should be raised at runtime: `serve.not_exist_prop', not found the property `not_exist_prop'.

  Scenario: yield edge
    When executing query:
      """
      FETCH PROP ON serve 'Boris Diaw' -> 'Hawks' YIELD serve.start_year, serve.end_year, edge as relationship
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year | relationship                                                         |
      | 2003             | 2005           | [:serve "Boris Diaw"->"Hawks" @0 {end_year: 2005, start_year: 2003}] |
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Spurs" YIELD edge as relationship, src(edge) as src_edge, dst(edge) as dst_edge, type(edge) as type, rank(edge) as rank
      """
    Then the result should be, in any order:
      | relationship                                                         | src_edge     | dst_edge | type    | rank |
      | [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}] | "Boris Diaw" | "Spurs"  | "serve" | 0    |
    When executing query:
      """
      FETCH PROP ON serve "Boris Diaw"->"Not Exist" YIELD src(edge) as a
      """
    Then the result should be, in any order:
      | a |
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" YIELD edge as relationship |
      YIELD properties($-.relationship)
      """
    Then the result should be, in any order:
      | properties($-.relationship) |
      | {likeness: 95}              |
      | {likeness: 90}              |
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" YIELD properties(edge) as properties
      """
    Then the result should be, in any order:
      | properties     |
      | {likeness: 95} |
      | {likeness: 90} |
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" YIELD edge as relationship |
      YIELD startNode($-.relationship) AS node
      """
    Then the result should be, in any order, with relax comparison:
      | node            |
      | ("Tony Parker") |
      | ("Grant Hill")  |
    When executing query:
      """
      FETCH PROP ON like "Tony Parker"->"Tim Duncan", "Grant Hill" -> "Tracy McGrady" YIELD edge as relationship |
      YIELD endNode($-.relationship) AS node
      """
    Then the result should be, in any order, with relax comparison:
      | node              |
      | ("Tim Duncan")    |
      | ("Tracy McGrady") |
    When executing query:
      """
      $var = GO FROM 'Boris Diaw','Boris Diaw' OVER serve YIELD serve._src AS src, serve._dst AS dst;
      FETCH PROP ON serve $var.src->$var.dst YIELD DISTINCT serve.start_year, serve.end_year, edge as relationship
      """
    Then the result should be, in any order:
      | serve.start_year | serve.end_year | relationship                                                           |
      | 2005             | 2008           | [:serve "Boris Diaw"->"Suns" @0 {end_year: 2008, start_year: 2005}]    |
      | 2003             | 2005           | [:serve "Boris Diaw"->"Hawks" @0 {end_year: 2005, start_year: 2003}]   |
      | 2012             | 2016           | [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]   |
      | 2008             | 2012           | [:serve "Boris Diaw"->"Hornets" @0 {end_year: 2012, start_year: 2008}] |
      | 2016             | 2017           | [:serve "Boris Diaw"->"Jazz" @0 {end_year: 2017, start_year: 2016}]    |

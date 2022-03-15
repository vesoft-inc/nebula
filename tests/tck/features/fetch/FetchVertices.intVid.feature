Feature: Fetch Int Vid Vertices

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Fetch prop on one tag of a vertex
    # return specific properties
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name, player.age, player.age > 30
      """
    Then the result should be, in any order:
      | player.name  | player.age | (player.age>30) |
      | "Boris Diaw" | 36         | True            |
    # without YIELD
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD vertex as node
      """
    Then the result should be, in any order:
      | node                                                  |
      | (hash('Boris Diaw'):player{age:36,name:"Boris Diaw"}) |
    # Fetch prop on not existing vertex
    When executing query:
      """
      FETCH PROP ON player hash('NON EXIST VERTEX ID') yield player.name
      """
    Then the result should be, in any order:
      | player.name |
    # work with ORDER BY
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') over like YIELD like._dst as id;
      FETCH PROP ON player $var.id YIELD player.name as name, player.age |
      ORDER BY $-.name
      """
    Then the result should be, in any order:
      | name          | player.age |
      | "Tim Duncan"  | 42         |
      | "Tony Parker" | 36         |
    # works with DISTINCT
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw'), hash('Boris Diaw') YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw'), hash('Boris Diaw') YIELD DISTINCT player.age
      """
    Then the result should be, in any order:
      | player.age |
      | 36         |

  Scenario: Fetch prop on multiple tags
    When executing query:
      """
      FETCH PROP ON bachelor, team, player hash("Tim Duncan"), hash("Boris Diaw") YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, with relax comparison:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
      | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      FETCH PROP ON player, team hash("Tim Duncan"),hash("Boris Diaw") YIELD player.name, team.name
      """
    Then the result should be, in any order:
      | player.name  | team.name |
      | "Boris Diaw" | EMPTY     |
      | "Tim Duncan" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON player, team hash("Boris Diaw"),hash("Boris Diaw") YIELD player.name, team.name
      """
    Then the result should be, in any order:
      | player.name  | team.name |
      | "Boris Diaw" | EMPTY     |
      | "Boris Diaw" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON team, player, bachelor hash("Boris Diaw") YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      FETCH PROP ON player, team, bachelor hash("Tim Duncan") YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order:
      | player.name  | team.name | bachelor.name |
      | "Tim Duncan" | EMPTY     | "Tim Duncan"  |

  Scenario: Fetch from pipe
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id |
      FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name   | player.age |
      | "Tony Parker" | 36         |
      | "Tim Duncan"  | 42         |
    # empty input
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') over like YIELD like._dst as id |
      FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | player.name |
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') over serve YIELD serve._dst as id, serve.start_year as start |
      YIELD $-.id as id WHERE $-.start > 20000 |
      FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | player.name |
    # Fetch prop on multi tags of vertices from pipe
    When executing query:
      """
      GO FROM hash("Boris Diaw") over like YIELD like._dst as id |
      FETCH PROP ON player, team, bachelor $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id |
      FETCH PROP ON player, bachelor $-.id YIELD player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name   | player.age | bachelor.name | bachelor.speciality |
      | "Tony Parker" | 36         | EMPTY         | EMPTY               |
      | "Tim Duncan"  | 42         | "Tim Duncan"  | "psychology"        |

  Scenario: Fetch Vertices works with variable.
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') over like YIELD like._dst as id; FETCH PROP ON player $var.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name   | player.age |
      | "Tony Parker" | 36         |
      | "Tim Duncan"  | 42         |
    # Fetch prop on multi tags of vertices from variable
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") over like YIELD like._dst as id;
      FETCH PROP ON player, team, bachelor $var.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
    # Fetch prop on * from variable
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") over like YIELD like._dst as id; FETCH PROP ON * $var.id YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order:
      | player.name   | team.name | bachelor.name |
      | "Tony Parker" | EMPTY     | EMPTY         |
      | "Tim Duncan"  | EMPTY     | "Tim Duncan"  |

  @skip
  Scenario: Fetch Vertices works with uuid() and YIELD
    When executing query:
      """
      FETCH PROP ON player uuid('Boris Diaw') YIELD player.name, player.age
      """
    Then the result should be, in any order, with relax comparison:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
    # without YIELD
    When executing query:
      """
      FETCH PROP ON player uuid('Boris Diaw')
      """
    Then the result should be, in any order, with relax comparison:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |

  Scenario: Fetch prop on *
    # on not existing vertex
    When executing query:
      """
      FETCH PROP ON * hash('NON EXIST VERTEX ID') yield player.name
      """
    Then the result should be, in any order:
      | player.name |
    When executing query:
      """
      FETCH PROP ON * hash('NON EXIST VERTEX ID') YIELD vertex as node
      """
    Then the result should be, in any order:
      | node |
    # on existing vertex
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw') yield player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
    # on multiple vertices
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw'), hash('Boris Diaw') yield player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * hash('Tim Duncan'), hash('Boris Diaw') yield player.name, bachelor.name
      """
    Then the result should be, in any order:
      | player.name  | bachelor.name |
      | "Boris Diaw" | EMPTY         |
      | "Tim Duncan" | "Tim Duncan"  |
    # from pipe
    When executing query:
      """
      YIELD hash('Boris Diaw') as id | FETCH PROP ON * $-.id yield player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      YIELD hash('Tim Duncan') as id | FETCH PROP ON * $-.id yield player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name  | player.age | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      FETCH PROP ON * hash('Tim Duncan') YIELD vertex as node
      """
    Then the result should be, in any order, with relax comparison:
      | node                           |
      | ("Tim Duncan":player:bachelor) |

  Scenario: Fetch and Yield id(v)
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw'), hash('Tony Parker') YIELD vertex as node | YIELD id($-.node) as id
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id            |
      | "Boris Diaw"  |
      | "Tony Parker" |

  Scenario: Fetch vertices and then GO
    When executing query:
      """
      FETCH PROP ON player hash('Tony Parker') YIELD player.name as Name, id(vertex) as id |
      GO FROM $-.id OVER like YIELD dst(edge) as dst
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | dst                 |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |

  Scenario: Typical errors
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD vertex
      """
    Then a SyntaxError should be raised at runtime: please add alias when using `vertex'. near `vertex'
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD edge as a
      """
    Then a SemanticError should be raised at runtime: illegal yield clauses `EDGE AS a'
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD src(edge)
      """
    Then a SemanticError should be raised at runtime: illegal yield clauses `src(EDGE)'
    # not support get src property
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD $^.player.name, player.age
      """
    Then a SemanticError should be raised at runtime: unsupported src/dst property expression in yield.
    # not support get dst property
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD $$.player.name, player.age
      """
    Then a SemanticError should be raised at runtime: unsupported src/dst property expression in yield.
    # yields not existing tag
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD not_exist_tag.name, player.age
      """
    Then a ExecutionError should be raised at runtime:
    # Fetch prop no not existing tag
    When executing query:
      """
      FETCH PROP ON not_exist_tag hash('Boris Diaw')
      """
    Then a ExecutionError should be raised at runtime:
    # yield not existing property
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.not_existing_prop
      """
    Then a SemanticError should be raised at runtime:
    # duplicate input
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id, like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then a SemanticError should be raised at runtime:
    # only constant list or single column of data is allowed in piped FETCH clause
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._src as src, like._dst as dst | FETCH PROP ON player $-.src, $-.dst YIELD vertex as node;
      """
    Then a SyntaxError should be raised at runtime:

  Scenario: Different from v1.x
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age, $-.*
      """
    Then a SemanticError should be raised at runtime:
    # $- is not supported
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') OVER serve YIELD dst(edge) as id | FETCH PROP ON team $- YIELD vertex as node
      """
    Then a SyntaxError should be raised at runtime:

  Scenario: format yield
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw') YIELD id(vertex)
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id(VERTEX)   |
      | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw') YIELD id(vertex), player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id(VERTEX)   | player.age |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw') YIELD id(vertex), player.age, vertex as node
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id(VERTEX)   | player.age | node                                             |
      | "Boris Diaw" | 36         | ("Boris Diaw":player{name:"Boris Diaw", age:36}) |
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw') YIELD vertex as node
      """
    Then the result should be, in any order:
      | node                                             |
      | ("Boris Diaw":player{name:"Boris Diaw", age:36}) |
    When executing query:
      """
      FETCH PROP ON * hash("Tim Duncan") YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality, vertex as node
      """
    Then the result should be, in any order:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality | node                                                                                                        |
      | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id |
      FETCH PROP ON * $-.id YIELD VERTEX as node
      """
    Then the result should be, in any order:
      | node                                                      |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
    When executing query:
      """
      FETCH PROP ON * hash('NON EXIST VERTEX ID'), hash('Boris Diaw') yield player.name, id(vertex)
      """
    Then the result should be, in any order, and the columns 1 should be hashed:
      | player.name  | id(VERTEX)   |
      | "Boris Diaw" | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON player hash('Tim Duncan') YIELD  id(vertex), properties(vertex).name as name, properties(vertex)
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id(VERTEX)   | name         | properties(VERTEX)            |
      | "Tim Duncan" | "Tim Duncan" | {age: 42, name: "Tim Duncan"} |
    When executing query:
      """
      FETCH PROP ON * hash('Tim Duncan') YIELD  id(vertex), keys(vertex) as keys, tags(vertex) as tags_, properties(vertex) as props
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id(VERTEX)   | keys                          | tags_                  | props                                                   |
      | "Tim Duncan" | ["age", "name", "speciality"] | ["bachelor", "player"] | {age: 42, name: "Tim Duncan", speciality: "psychology"} |

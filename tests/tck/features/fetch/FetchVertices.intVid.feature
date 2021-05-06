Feature: Fetch Int Vid Vertices

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Fetch prop on one tag of a vertex
    # return specific properties
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name, player.age, player.age > 30
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age | (player.age>30) |
      | "Boris Diaw" | "Boris Diaw" | 36         | True            |
    # without YIELD
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw')
      """
    Then the result should be, in any order:
      | vertices_                                             |
      | (hash('Boris Diaw'):player{age:36,name:"Boris Diaw"}) |
    # Fetch prop on not existing vertex
    When executing query:
      """
      FETCH PROP ON player hash('NON EXIST VERTEX ID') yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    # work with ORDER BY
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') over like YIELD like._dst as id;
      FETCH PROP ON player $var.id YIELD player.name as name, player.age |
      ORDER BY name
      """
    Then the result should be, in order, and the columns 0 should be hashed:
      | VertexID      | name          | player.age |
      | "Tim Duncan"  | "Tim Duncan"  | 42         |
      | "Tony Parker" | "Tony Parker" | 36         |
    # works with DISTINCT
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw'), hash('Boris Diaw') YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw'), hash('Boris Diaw') YIELD DISTINCT player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.age |
      | "Boris Diaw" | 36         |

  Scenario: Fetch prop on multiple tags
    When executing query:
      """
      FETCH PROP ON bachelor, team, player hash("Tim Duncan"), hash("Boris Diaw") YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
      | "Tim Duncan" | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      FETCH PROP ON player, team hash("Tim Duncan"),hash("Boris Diaw") YIELD player.name, team.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | team.name |
      | "Boris Diaw" | "Boris Diaw" | EMPTY     |
      | "Tim Duncan" | "Tim Duncan" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON player, team hash("Boris Diaw"),hash("Boris Diaw") YIELD player.name, team.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | team.name |
      | "Boris Diaw" | "Boris Diaw" | EMPTY     |
      | "Boris Diaw" | "Boris Diaw" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON team, player, bachelor hash("Boris Diaw") YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      FETCH PROP ON player, team, bachelor hash("Tim Duncan") YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | team.name | bachelor.name |
      | "Tim Duncan" | "Tim Duncan" | EMPTY     | "Tim Duncan"  |

  Scenario: Fetch from pipe
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
      | "Tim Duncan"  | "Tim Duncan"  | 42         |
    # empty input
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') over like YIELD like._dst as id | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') over serve YIELD serve._dst as id, serve.start_year as start
      | YIELD $-.id as id WHERE $-.start > 20000 | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    # Fetch prop on multi tags of vertices from pipe
    When executing query:
      """
      GO FROM hash("Boris Diaw") over like YIELD like._dst as id
      | FETCH PROP ON player, team, bachelor $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Tony Parker" | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id
      | FETCH PROP ON player, bachelor $-.id YIELD player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      | player.name   | player.age | bachelor.name | bachelor.speciality |
      | "Tony Parker" | "Tony Parker" | 36         | EMPTY         | EMPTY               |
      | "Tim Duncan"  | "Tim Duncan"  | 42         | "Tim Duncan"  | "psychology"        |

  Scenario: Fetch Vertices works with variable.
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') over like YIELD like._dst as id; FETCH PROP ON player $var.id YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
      | "Tim Duncan"  | "Tim Duncan"  | 42         |
    # Fetch prop on multi tags of vertices from variable
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") over like YIELD like._dst as id;
      FETCH PROP ON player, team, bachelor $var.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Tony Parker" | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
    # Fetch prop on * from variable
    When executing query:
      """
      $var = GO FROM hash("Boris Diaw") over like YIELD like._dst as id; FETCH PROP ON * $var.id YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      | player.name   | team.name | bachelor.name |
      | "Tony Parker" | "Tony Parker" | EMPTY     | EMPTY         |
      | "Tim Duncan"  | "Tim Duncan"  | EMPTY     | "Tim Duncan"  |

  @skip
  Scenario: Fetch Vertices works with uuid() and YIELD
    When executing query:
      """
      FETCH PROP ON player uuid('Boris Diaw') YIELD player.name, player.age
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID             | player.name  | player.age |
      | -7391649757168799460 | "Boris Diaw" | 36         |
    # without YIELD
    When executing query:
      """
      FETCH PROP ON player uuid('Boris Diaw')
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID             | player.name  | player.age |
      | -7391649757168799460 | "Boris Diaw" | 36         |

  Scenario: Fetch prop on *
    # on not existing vertex
    When executing query:
      """
      FETCH PROP ON * hash('NON EXIST VERTEX ID') yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    When executing query:
      """
      FETCH PROP ON * hash('NON EXIST VERTEX ID')
      """
    Then the result should be, in any order:
      | vertices_ |
    # on existing vertex
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw') yield player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    # on multiple vertices
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw'), hash('Boris Diaw') yield player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * hash('Tim Duncan'), hash('Boris Diaw') yield player.name, bachelor.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | bachelor.name |
      | "Boris Diaw" | "Boris Diaw" | EMPTY         |
      | "Tim Duncan" | "Tim Duncan" | "Tim Duncan"  |
    # from pipe
    When executing query:
      """
      YIELD hash('Boris Diaw') as id | FETCH PROP ON * $-.id yield player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    When executing query:
      """
      YIELD hash('Tim Duncan') as id | FETCH PROP ON * $-.id yield player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      FETCH PROP ON * hash('Tim Duncan')
      """
    Then the result should be, in any order, with relax comparison:
      | vertices_                      |
      | ("Tim Duncan":player:bachelor) |

  Scenario: Fetch and Yield id(v)
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw'), hash('Tony Parker') | YIELD id($-.vertices_) as id
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id            |
      | "Boris Diaw"  |
      | "Tony Parker" |

  Scenario: Fetch vertices and then GO
    When executing query:
      """
      FETCH PROP ON player hash('Tony Parker') YIELD player.name as Name
      | GO FROM $-.VertexID OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |

  Scenario: Typical errors
    # not support get src property
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD $^.player.name, player.age
      """
    Then a SemanticError should be raised at runtime: Unsupported src/dst property expression in yield.
    # not support get dst property
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD $$.player.name, player.age
      """
    Then a SemanticError should be raised at runtime: Unsupported src/dst property expression in yield.
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

  Scenario: Different from v1.x
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age, $-.*
      """
    Then a SemanticError should be raised at runtime:
    # $- is not supported
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') OVER serve | FETCH PROP ON team $-
      """
    Then a SyntaxError should be raised at runtime:

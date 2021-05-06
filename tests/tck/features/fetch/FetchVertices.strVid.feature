Feature: Fetch String Vertices

  Background:
    Given a graph with space named "nba"

  Scenario: Fetch prop on one tag, one vertex
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    # Fetch Vertices yield duplicate columns
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.name  |
      | "Boris Diaw" | "Boris Diaw" | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.age, player.age > 30
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | (player.age>30) |
      | "Boris Diaw" | "Boris Diaw" | 36         | True            |
    # Fetch Vertices without YIELD
    When executing query:
      """
      FETCH PROP ON bachelor 'Tim Duncan'
      """
    Then the result should be, in any order:
      | vertices_                                                          |
      | ("Tim Duncan":bachelor{name:"Tim Duncan",speciality:"psychology"}) |
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw'
      """
    Then the result should be, in any order:
      | vertices_                                        |
      | ("Boris Diaw":player{name:"Boris Diaw", age:36}) |

  Scenario: Fetch Vertices works with ORDER BY
    When executing query:
      """
      $var = GO FROM 'Boris Diaw' over like YIELD like._dst as id;
      FETCH PROP ON player $var.id YIELD player.name as name, player.age |
      ORDER BY name
      """
    Then the result should be, in order:
      | VertexID      | name          | player.age |
      | "Tim Duncan"  | "Tim Duncan"  | 42         |
      | "Tony Parker" | "Tony Parker" | 36         |

  Scenario: Fetch Vertices works with DISTINCT
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw', 'Boris Diaw' YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON player "Boris Diaw", "Tony Parker" YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age |
      | "Boris Diaw"  | "Boris Diaw"  | 36         |
      | "Tony Parker" | "Tony Parker" | 36         |
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw', 'Boris Diaw' YIELD DISTINCT player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.age |
      | "Boris Diaw" | 36         |

  Scenario: Fetch prop on multiple tags, multiple vertices
    When executing query:
      """
      FETCH PROP ON bachelor, team, player "Tim Duncan", "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
      | "Tim Duncan" | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      FETCH PROP ON player, team "Tim Duncan","Boris Diaw" YIELD player.name, team.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | team.name |
      | "Boris Diaw" | "Boris Diaw" | EMPTY     |
      | "Tim Duncan" | "Tim Duncan" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON player, team "Boris Diaw","Boris Diaw" YIELD player.name, team.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | team.name |
      | "Boris Diaw" | "Boris Diaw" | EMPTY     |
      | "Boris Diaw" | "Boris Diaw" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON team, player, bachelor "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      FETCH PROP ON player, team, bachelor "Tim Duncan" YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | team.name | bachelor.name |
      | "Tim Duncan" | "Tim Duncan" | EMPTY     | "Tim Duncan"  |

  Scenario: Fetch prop on not existing vertex
    When executing query:
      """
      FETCH PROP ON player 'NON EXIST VERTEX ID' yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    When executing query:
      """
      FETCH PROP ON player 'NON EXIST VERTEX ID', "Boris Diaw" yield player.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  |
      | "Boris Diaw" | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON * 'NON EXIST VERTEX ID'
      """
    Then the result should be, in any order:
      | vertices_ |

  Scenario: Fetch prop on *
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' yield player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' yield player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
    # Fetch prop on * with not existing vertex
    When executing query:
      """
      FETCH PROP ON * 'NON EXIST VERTEX ID' yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    When executing query:
      """
      FETCH PROP ON * 'NON EXIST VERTEX ID', 'Boris Diaw' yield player.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  |
      | "Boris Diaw" | "Boris Diaw" |
    # Fetch prop on * with multiple vertices
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw', 'Boris Diaw' yield player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
      | "Boris Diaw" | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * 'Tim Duncan', 'Boris Diaw' YIELD player.name, bachelor.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | bachelor.name |
      | "Tim Duncan" | "Tim Duncan" | "Tim Duncan"  |
      | "Boris Diaw" | "Boris Diaw" | EMPTY         |
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Boris Diaw" | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |

  Scenario: Fetch from pipe
    # Fetch prop on one tag of vertices from pipe
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
      | "Tim Duncan"  | "Tim Duncan"  | 42         |
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id
      | FETCH PROP ON player, bachelor $-.id YIELD player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age | bachelor.name | bachelor.speciality |
      | "Tony Parker" | "Tony Parker" | 36         | EMPTY         | EMPTY               |
      | "Tim Duncan"  | "Tim Duncan"  | 42         | "Tim Duncan"  | "psychology"        |
    # Fetch prop on multi tags of vertices from pipe
    When executing query:
      """
      GO FROM "Boris Diaw" over like YIELD like._dst as id
      | FETCH PROP ON player, team, bachelor $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Tony Parker" | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id
      | FETCH PROP ON player, bachelor $-.id YIELD player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age | bachelor.name | bachelor.speciality |
      | "Tony Parker" | "Tony Parker" | 36         | EMPTY         | EMPTY               |
      | "Tim Duncan"  | "Tim Duncan"  | 42         | "Tim Duncan"  | "psychology"        |
    # Fetch vertices with empty input
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' over like YIELD like._dst as id | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' over serve YIELD serve._dst as id, serve.start_year as start
      | YIELD $-.id as id WHERE $-.start > 20000 | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    # Fetch * from input
    When executing query:
      """
      YIELD 'Tim Duncan' as id | FETCH PROP ON * $-.id yield player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      GO FROM "Boris Diaw" over like YIELD like._dst as id
      | FETCH PROP ON * $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tony Parker" | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
      | "Tim Duncan"  | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |

  Scenario: fetch from varibles
    When executing query:
      """
      $var = GO FROM 'Boris Diaw' over like YIELD like._dst as id; FETCH PROP ON player $var.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
      | "Tim Duncan"  | "Tim Duncan"  | 42         |
    When executing query:
      """
      $var = GO FROM "Boris Diaw" over like YIELD like._dst as id; FETCH PROP ON * $var.id YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | team.name | bachelor.name |
      | "Tony Parker" | "Tony Parker" | EMPTY     | EMPTY         |
      | "Tim Duncan"  | "Tim Duncan"  | EMPTY     | "Tim Duncan"  |
    # Fetch prop on multi tags of vertices from variable
    When executing query:
      """
      $var = GO FROM "Boris Diaw" over like YIELD like._dst as id;
      FETCH PROP ON player, team, bachelor $var.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Tony Parker" | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |

  Scenario: Fetch and Yield id(v)
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw', 'Tony Parker' | YIELD id($-.vertices_) as id
      """
    Then the result should be, in any order:
      | id            |
      | "Boris Diaw"  |
      | "Tony Parker" |

  @skip
  Scenario: Output fetch result to graph traverse
    When executing query:
      """
      FETCH PROP ON player 'NON EXIST VERTEX ID' | go from id($-.vertices_) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" | go from id($-.vertices_) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst       |
      | "Manu Ginobili" |
      | "Tony Parker"   |
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan", "Yao Ming" | go from id($-.vertices_) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst         |
      | "Shaquile O'Neal" |
      | "Tracy McGrady"   |
      | "Manu Ginobili"   |
      | "Tony Parker"     |
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" yield player.name as id | go from $-.id over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst       |
      | "Manu Ginobili" |
      | "Tony Parker"   |
    When executing query:
      """
      $var = FETCH PROP ON player "Tim Duncan", "Yao Ming"; go from id($var.vertices_) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst         |
      | "Manu Ginobili"   |
      | "Tony Parker"     |
      | "Shaquile O'Neal" |
      | "Tracy McGrady"   |
    When executing query:
      """
      FETCH PROP ON player 'Tony Parker' YIELD player.name as Name
      | GO FROM $-.Name OVER like
      """
    Then the result should be, in any order:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
    When executing query:
      """
      FETCH PROP ON player 'Tony Parker' YIELD player.name as Name
      | GO FROM $-.VertexID OVER like
      """
    Then the result should be, in any order:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |

  Scenario: Typical errors
    # Fetch Vertices not support get src property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD $^.player.name, player.age
      """
    Then a SemanticError should be raised at runtime:
    # Fetch Vertices not support get dst property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD $$.player.name, player.age
      """
    Then a SemanticError should be raised at runtime:
    # Fetch vertex yields not existing tag
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD not_exist_tag.name, player.age
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD not_exist_tag.name
      """
    Then a ExecutionError should be raised at runtime:
    # Fetch prop no not existing tag
    When executing query:
      """
      FETCH PROP ON not_exist_tag 'Boris Diaw'
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id, like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM "11" over like YIELD like._dst as id | FETCH PROP ON player "11" YIELD $-.id
      """
    Then a SemanticError should be raised at runtime:
    # Fetch on existing vertex, and yield not existing property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.not_exist_prop
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD player.not_exist_prop
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Different from v1.x
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age, $-.*
      """
    Then a SemanticError should be raised at runtime:
    # Different from 1.x $- is not supported
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' OVER serve | FETCH PROP ON team $-
      """
    Then a SyntaxError should be raised at runtime:

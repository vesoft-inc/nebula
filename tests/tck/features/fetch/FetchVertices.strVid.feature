Feature: Fetch String Vertices

  Background:
    Given a graph with space named "nba"

  Scenario: [1] Fetch prop on one tag of a vertex and return the specific properties
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |

  Scenario: [2] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.age, player.age > 30
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | (player.age>30) |
      | "Boris Diaw" | "Boris Diaw" | 36         | True            |

  Scenario: [3] Fetch dst vertices props of go traversal.
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
      | "Tim Duncan"  | "Tim Duncan"  | 42         |

  Scenario: [4] Fetch Vertices, different from v1.x
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age, $-.*
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [5] Fetch Vertices works with variable.
    When executing query:
      """
      $var = GO FROM 'Boris Diaw' over like YIELD like._dst as id; FETCH PROP ON player $var.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
      | "Tim Duncan"  | "Tim Duncan"  | 42         |

  Scenario: [6] Fetch Vertices works with ORDER BY
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

  Scenario: [9] Fetch Vertices without YIELD
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw'
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |

  Scenario: [12] Fetch Vertices works with DISTINCT
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw', 'Boris Diaw' YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |

  Scenario: [13] Fetch Vertices works with DISTINCT
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw', 'Boris Diaw' YIELD DISTINCT player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.age |
      | "Boris Diaw" | 36         |

  Scenario: [14] Fetch Vertices not support get src property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD $^.player.name, player.age
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [15] Fetch Vertices not support get dst property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD $$.player.name, player.age
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [16] Fetch vertex yields not existing tag
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD abc.name, player.age
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: [17] Fetch prop no not existing tag
    When executing query:
      """
      FETCH PROP ON abc 'Boris Diaw'
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: [18] Fetch prop on not existing vertex
    When executing query:
      """
      FETCH PROP ON player 'NON EXIST VERTEX ID' yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |

  Scenario: [19] Fetch prop on not existing vertex. Different from 1.x $- is not supported
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' OVER serve | FETCH PROP ON team $-
      """
    Then a SyntaxError should be raised at runtime:

  Scenario: [20] Fetch prop on * with not existing vertex
    When executing query:
      """
      FETCH PROP ON * 'NON EXIST VERTEX ID' yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |

  Scenario: [21] Fetch prop on * with existing vertex
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' yield player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |

  Scenario: [22] Fetch prop works with pipeline
    When executing query:
      """
      YIELD 'Boris Diaw' as id | FETCH PROP ON * $-.id yield player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |

  Scenario: [23] Fetch prop on multiple vertices
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw', 'Boris Diaw' yield player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Boris Diaw" | "Boris Diaw" | 36         |
      | "Boris Diaw" | "Boris Diaw" | 36         |

  Scenario: [24] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON bachelor 'Tim Duncan'
      """
    Then the result should be, in any order:
      | VertexID     | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"  | "psychology"        |

  Scenario: [25] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * 'Tim Duncan' yield player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |

  Scenario: [26] Fetch Vertices
    When executing query:
      """
      YIELD 'Tim Duncan' as id | FETCH PROP ON * $-.id yield player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |

  Scenario: [27] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * 'Tim Duncan', 'Tim Duncan' yield player.name, bachelor.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | bachelor.name |
      | "Tim Duncan" | "Tim Duncan" | "Tim Duncan"  |
      | "Tim Duncan" | "Tim Duncan" | "Tim Duncan"  |

  Scenario: [28] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * 'Tim Duncan', 'Tim Duncan' YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
      | "Tim Duncan" | "Tim Duncan" | 42         |

  Scenario: [29] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.name
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.name  |
      | "Boris Diaw" | "Boris Diaw" | "Boris Diaw" |

  Scenario: [30] Fetch Vertices
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id, like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [31] Fetch on existing vertex, and yield not existing property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name1
      """
    Then a SemanticError should be raised at runtime:

  Scenario: [32] Fetch Vertices
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' over like YIELD like._dst as id | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |

  Scenario: [33] Fetch Vertices
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' over serve YIELD serve._dst as id, serve.start_year as start
      | YIELD $-.id as id WHERE $-.start > 20000 | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |

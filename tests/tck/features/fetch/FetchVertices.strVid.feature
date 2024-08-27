Feature: Fetch String Vertices

  Background:
    Given a graph with space named "nba"

  Scenario: Fetch prop on one tag, one vertex
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
    # Fetch Vertices yield duplicate columns
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.name
      """
    Then the result should be, in any order:
      | player.name  | player.name  |
      | "Boris Diaw" | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.name, player.age, player.age > 30
      """
    Then the result should be, in any order:
      | player.name  | player.age | (player.age>30) |
      | "Boris Diaw" | 36         | True            |
    # Fetch Vertices without YIELD
    When executing query:
      """
      FETCH PROP ON bachelor 'Tim Duncan' YIELD vertex as node
      """
    Then the result should be, in any order:
      | node                                                               |
      | ("Tim Duncan":bachelor{name:"Tim Duncan",speciality:"psychology"}) |
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD vertex as node
      """
    Then the result should be, in any order:
      | node                                             |
      | ("Boris Diaw":player{name:"Boris Diaw", age:36}) |

  Scenario: Fetch Vertices works with ORDER BY
    When executing query:
      """
      $var = GO FROM 'Boris Diaw' over like YIELD like._dst as id;
      FETCH PROP ON player $var.id YIELD player.name as name, player.age |
      ORDER BY $-.name
      """
    Then the result should be, in order:
      | name          | player.age |
      | "Tim Duncan"  | 42         |
      | "Tony Parker" | 36         |

  Scenario: Fetch Vertices works with DISTINCT
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw', 'Boris Diaw' YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON player "Boris Diaw", "Tony Parker" YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order:
      | player.name   | player.age |
      | "Boris Diaw"  | 36         |
      | "Tony Parker" | 36         |
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw', 'Boris Diaw' YIELD DISTINCT player.age
      """
    Then the result should be, in any order:
      | player.age |
      | 36         |

  Scenario: Fetch prop on multiple tags, multiple vertices
    When executing query:
      """
      FETCH PROP ON bachelor, team, player "Tim Duncan", "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
      | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      FETCH PROP ON player, team "Tim Duncan","Boris Diaw" YIELD player.name, team.name
      """
    Then the result should be, in any order:
      | player.name  | team.name |
      | "Boris Diaw" | EMPTY     |
      | "Tim Duncan" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON player, team "Boris Diaw","Boris Diaw" YIELD player.name, team.name
      """
    Then the result should be, in any order:
      | player.name  | team.name |
      | "Boris Diaw" | EMPTY     |
      | "Boris Diaw" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON team, player, bachelor "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      FETCH PROP ON player, team, bachelor "Tim Duncan" YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order:
      | player.name  | team.name | bachelor.name |
      | "Tim Duncan" | EMPTY     | "Tim Duncan"  |

  Scenario: Fetch prop on not existing vertex
    When executing query:
      """
      FETCH PROP ON player 'NON EXIST VERTEX ID' yield player.name
      """
    Then the result should be, in any order:
      | player.name |
    When executing query:
      """
      FETCH PROP ON player 'NON EXIST VERTEX ID', "Boris Diaw" yield player.name
      """
    Then the result should be, in any order:
      | player.name  |
      | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON * 'NON EXIST VERTEX ID' YIELD vertex as node
      """
    Then the result should be, in any order:
      | node |

  Scenario: Fetch prop on *
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' yield player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' yield player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
    # Fetch prop on * with not existing vertex
    When executing query:
      """
      FETCH PROP ON * 'NON EXIST VERTEX ID' yield player.name
      """
    Then the result should be, in any order:
      | player.name |
    When executing query:
      """
      FETCH PROP ON * 'NON EXIST VERTEX ID', 'Boris Diaw' yield player.name
      """
    Then the result should be, in any order:
      | player.name  |
      | "Boris Diaw" |
    # Fetch prop on * with multiple vertices
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw', 'Boris Diaw' yield player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Boris Diaw" | 36         |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * 'Tim Duncan', 'Boris Diaw' YIELD player.name, bachelor.name
      """
    Then the result should be, in any order:
      | player.name  | bachelor.name |
      | "Tim Duncan" | "Tim Duncan"  |
      | "Boris Diaw" | EMPTY         |
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |

  Scenario: Fetch from pipe
    # Fetch prop on one tag of vertices from pipe
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name   | player.age |
      | "Tony Parker" | 36         |
      | "Tim Duncan"  | 42         |
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id |
      FETCH PROP ON player, bachelor $-.id YIELD player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name   | player.age | bachelor.name | bachelor.speciality |
      | "Tony Parker" | 36         | EMPTY         | EMPTY               |
      | "Tim Duncan"  | 42         | "Tim Duncan"  | "psychology"        |
    # Fetch prop on multi tags of vertices from pipe
    When executing query:
      """
      GO FROM "Boris Diaw" over like YIELD like._dst as id |
      FETCH PROP ON player, team, bachelor $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id |
      FETCH PROP ON player, bachelor $-.id YIELD player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name   | player.age | bachelor.name | bachelor.speciality |
      | "Tony Parker" | 36         | EMPTY         | EMPTY               |
      | "Tim Duncan"  | 42         | "Tim Duncan"  | "psychology"        |
    # Fetch vertices with empty input
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' over like YIELD like._dst as id | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | player.name |
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' over serve YIELD serve._dst as id, serve.start_year as start |
      YIELD $-.id as id WHERE $-.start > 20000 |
      FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | player.name |
    # Fetch * from input
    When executing query:
      """
      YIELD 'Tim Duncan' as id | FETCH PROP ON * $-.id yield player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name  | player.age | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      GO FROM "Boris Diaw" over like YIELD like._dst as id |
      FETCH PROP ON * $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
      | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |

  Scenario: fetch from variables
    When executing query:
      """
      $var = GO FROM 'Boris Diaw' over like YIELD like._dst as id; FETCH PROP ON player $var.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name   | player.age |
      | "Tony Parker" | 36         |
      | "Tim Duncan"  | 42         |
    When executing query:
      """
      $var = GO FROM "Boris Diaw" over like YIELD like._dst as id; FETCH PROP ON * $var.id YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order:
      | player.name   | team.name | bachelor.name |
      | "Tony Parker" | EMPTY     | EMPTY         |
      | "Tim Duncan"  | EMPTY     | "Tim Duncan"  |
    # Fetch prop on multi tags of vertices from variable
    When executing query:
      """
      $var = GO FROM "Boris Diaw" over like YIELD like._dst as id;
      FETCH PROP ON player, team, bachelor $var.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |

  Scenario: Fetch and Yield id(v)
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw', 'Tony Parker' YIELD vertex as node | YIELD id($-.node) as id
      """
    Then the result should be, in any order:
      | id            |
      | "Boris Diaw"  |
      | "Tony Parker" |

  @skip
  Scenario: Output fetch result to graph traverse
    When executing query:
      """
      FETCH PROP ON player 'NON EXIST VERTEX ID' YIELD vertex as node | go from id($-.node) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD vertex as node | go from id($-.node) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst       |
      | "Manu Ginobili" |
      | "Tony Parker"   |
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan", "Yao Ming" YIELD vertex as node | go from id($-.node) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst          |
      | "Shaquille O'Neal" |
      | "Tracy McGrady"    |
      | "Manu Ginobili"    |
      | "Tony Parker"      |
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
      | like._dst          |
      | "Manu Ginobili"    |
      | "Tony Parker"      |
      | "Shaquille O'Neal" |
      | "Tracy McGrady"    |
    When executing query:
      """
      FETCH PROP ON player 'Tony Parker' YIELD player.name as Name |
      GO FROM $-.Name OVER like YIELD like._dst
      """
    Then the result should be, in any order:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
    When executing query:
      """
      FETCH PROP ON player 'Tony Parker' YIELD id(vertex) as id, player.name as Name |
      GO FROM $-.id OVER like YIELD dst(edge) as dst
      """
    Then the result should be, in any order:
      | dst                 |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |

  Scenario: Typical errors
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD vertex
      """
    Then a SyntaxError should be raised at runtime: please add alias when using `vertex'. near `vertex'
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD edge as a
      """
    Then a SemanticError should be raised at runtime: illegal yield clauses `EDGE AS a'
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD src(edge)
      """
    Then a SemanticError should be raised at runtime: illegal yield clauses `src(EDGE)'
    # Fetch Vertices not support get src property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD $^.player.name, player.age
      """
    Then a SemanticError should be raised at runtime: unsupported src/dst property expression in yield.
    # Fetch Vertices not support get dst property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD $$.player.name, player.age
      """
    Then a SemanticError should be raised at runtime: unsupported src/dst property expression in yield.
    # Fetch vertex yields not existing tag
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD not_exist_tag.name, player.age
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `not_exist_tag`
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD not_exist_tag.name
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `not_exist_tag`
    # Fetch prop no not existing tag
    When executing query:
      """
      FETCH PROP ON not_exist_tag 'Boris Diaw'
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `not_exist_tag`
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id, like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'
    When executing query:
      """
      GO FROM "11" over like YIELD like._dst as id | FETCH PROP ON player "11" YIELD $-.id
      """
    Then a SemanticError should be raised at runtime: unsupported input/variable property expression in yield.
    # Fetch on existing vertex, and yield not existing property
    When executing query:
      """
      FETCH PROP ON player 'Boris Diaw' YIELD player.not_exist_prop
      """
    Then a SemanticError should be raised at runtime: `player.not_exist_prop', not found the property `not_exist_prop'.
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan", "Boris Diaw" YIELD player.not_exist_prop
      """
    Then a SemanticError should be raised at runtime: `player.not_exist_prop', not found the property `not_exist_prop'.
    # only constant list or single column of data is allowed in piped FETCH clause
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._src as src, like._dst as dst | FETCH PROP ON player $-.src, $-.dst;
      """
    Then a SyntaxError should be raised at runtime: syntax error near `, $-.dst'

  Scenario: Different from v1.x
    When executing query:
      """
      GO FROM 'Boris Diaw' over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age, $-.*
      """
    Then a SemanticError should be raised at runtime: `$-.*', not exist prop `*'
    # Different from 1.x $- is not supported
    When executing query:
      """
      GO FROM 'NON EXIST VERTEX ID' OVER serve | FETCH PROP ON team $-
      """
    Then a SyntaxError should be raised at runtime: syntax error near `$-'

  Scenario: format yield
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' YIELD id(vertex)
      """
    Then the result should be, in any order:
      | id(VERTEX)   |
      | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' YIELD id(vertex), player.age
      """
    Then the result should be, in any order:
      | id(VERTEX)   | player.age |
      | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' YIELD id(vertex), player.age, vertex as node
      """
    Then the result should be, in any order:
      | id(VERTEX)   | player.age | node                                             |
      | "Boris Diaw" | 36         | ("Boris Diaw":player{name:"Boris Diaw", age:36}) |
    When executing query:
      """
      FETCH PROP ON * 'Boris Diaw' YIELD vertex as node
      """
    Then the result should be, in any order:
      | node                                             |
      | ("Boris Diaw":player{name:"Boris Diaw", age:36}) |
    When executing query:
      """
      FETCH PROP ON * "Tim Duncan" YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality, vertex as node
      """
    Then the result should be, in any order:
      | player.name  | player.age | team.name | bachelor.name | bachelor.speciality | node                                                                                                        |
      | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst as id |
      FETCH PROP ON * $-.id YIELD vertex as node
      """
    Then the result should be, in any order:
      | node                                                      |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
    When executing query:
      """
      FETCH PROP ON * 'NON EXIST VERTEX ID', 'Boris Diaw' yield player.name, id(vertex)
      """
    Then the result should be, in any order:
      | player.name  | id(VERTEX)   |
      | "Boris Diaw" | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON player 'Tim Duncan' YIELD  id(vertex), properties(vertex).name as name, properties(vertex)
      """
    Then the result should be, in any order:
      | id(VERTEX)   | name         | properties(VERTEX)            |
      | "Tim Duncan" | "Tim Duncan" | {age: 42, name: "Tim Duncan"} |
    When executing query:
      """
      FETCH PROP ON * 'Tim Duncan' YIELD  id(vertex), keys(vertex) as keys, tags(vertex) as tags_, properties(vertex) as props
      """
    Then the result should be, in any order:
      | id(VERTEX)   | keys                          | tags_                  | props                                                   |
      | "Tim Duncan" | ["age", "name", "speciality"] | ["bachelor", "player"] | {age: 42, name: "Tim Duncan", speciality: "psychology"} |

  Scenario: Fetch properties from player tag with Lists
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE TAG player(name string, age int, hobby List< string >, ids List< int >, score List< float >);
      """
    Then the execution should be successful
    # Ensure the tag is successfully created
    And wait 3 seconds
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, ["Basketball", "Swimming"], [1, 2], [9.0, 8.0]);
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.name, player.age, player.hobby, player.ids, player.score;
      """
    Then the result should be, in any order, with relax comparison:
      | player.name  | player.age | player.hobby               | player.ids | player.score |
      | "Tim Duncan" | 42         | ["Basketball", "Swimming"] | [1, 2]     | [9.0, 8.0]   |
    When executing query:
      """
      FETCH PROP ON * "player100" YIELD player.name, player.age, player.hobby, player.ids, player.score, vertex as node;
      """
    Then the result should be, in any order:
      | player.name  | player.age | player.hobby               | player.ids | player.score | node                                                                                                                  |
      | "Tim Duncan" | 42         | ["Basketball", "Swimming"] | [1, 2]     | [9.0, 8.0]   | ("player100" :player{age: 42, hobby: ["Basketball", "Swimming"], ids: [1, 2], score: [9.0, 8.0], name: "Tim Duncan"}) |
    # Fetch vertex ID and properties on player100
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD id(vertex) as id, properties(vertex).name as name, properties(vertex);
      """
    Then the result should be, in any order:
      | id          | name         | properties(VERTEX)                                                                               |
      | "player100" | "Tim Duncan" | {age: 42, hobby: ["Basketball", "Swimming"], ids: [1, 2], score: [9.0, 8.0], name: "Tim Duncan"} |
    # Fetch vertex ID, keys, tags, and properties on player100
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD id(vertex), keys(vertex) as keys, tags(vertex) as tags_, properties(vertex) as props;
      """
    Then the result should be, in any order:
      | id(VERTEX)  | keys                                     | tags_      | props                                                                                            |
      | "player100" | ["age", "hobby", "ids", "score", "name"] | ["player"] | {age: 42, hobby: ["Basketball", "Swimming"], ids: [1, 2], score: [9.0, 8.0], name: "Tim Duncan"} |

  Scenario: Fetch properties from player tag with Sets
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE TAG player(name string, age int, hobby Set< string >, ids Set< int >, score Set< float >);
      """
    Then the execution should be successful
    # Ensure the tag is successfully created
    And wait 3 seconds
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, {"Basketball", "Swimming", "Basketball"}, {1, 2, 2}, {9.0, 8.0, 8.0});
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.name, player.age, player.hobby, player.ids, player.score;
      """
    Then the result should be, in any order, with relax comparison:
      | player.name  | player.age | player.hobby               | player.ids | player.score |
      | "Tim Duncan" | 42         | {"Basketball", "Swimming"} | {1, 2}     | {9.0, 8.0}   |
    When executing query:
      """
      FETCH PROP ON * "player100" YIELD player.name, player.age, player.hobby, player.ids, player.score, vertex as node;
      """
    Then the result should be, in any order:
      | player.name  | player.age | player.hobby               | player.ids | player.score | node                                                                                                                  |
      | "Tim Duncan" | 42         | {"Basketball", "Swimming"} | {1, 2}     | {9.0, 8.0}   | ("player100" :player{age: 42, hobby: {"Basketball", "Swimming"}, ids: {1, 2}, score: {9.0, 8.0}, name: "Tim Duncan"}) |
    # Fetch vertex ID and properties on player100
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD id(vertex) as id, properties(vertex).name as name, properties(vertex);
      """
    Then the result should be, in any order:
      | id          | name         | properties(VERTEX)                                                                               |
      | "player100" | "Tim Duncan" | {age: 42, hobby: {"Basketball", "Swimming"}, ids: {1, 2}, score: {9.0, 8.0}, name: "Tim Duncan"} |
    # Fetch vertex ID, keys, tags, and properties on player100
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD id(vertex), keys(vertex) as keys, tags(vertex) as tags_, properties(vertex) as props;
      """
    Then the result should be, in any order:
      | id(VERTEX)  | keys                                     | tags_      | props                                                                                            |
      | "player100" | ["age", "hobby", "ids", "score", "name"] | ["player"] | {age: 42, hobby: {"Basketball", "Swimming"}, ids: {1, 2}, score: {9.0, 8.0}, name: "Tim Duncan"} |

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Delete int vid of vertex

  Scenario: delete int vid vertex
    Given load "nba_int_vid" csv data to a new space
    # get vertex info
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst     |
      | "Tony Parker" |
      | "Tim Duncan"  |
    When executing query:
      """
      GO FROM hash("Tony Parker") OVER like REVERSELY
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Boris Diaw"        |
      | "Dejounte Murray"   |
      | "Marco Belinelli"   |
      | "Tim Duncan"        |
    # get value by fetch
    When executing query:
      """
      FETCH PROP ON player hash("Tony Parker") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
    # check value by fetch
    When executing query:
      """
      FETCH PROP ON serve hash("Tony Parker")->hash("Spurs") YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | serve._src    | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | 'Tony Parker' | 'Spurs'    | 0           | 1999             | 2018           |
    # delete one vertex
    When executing query:
      """
      DELETE VERTEX hash("Tony Parker");
      """
    Then the execution should be successful
    # after delete to check value by fetch
    When executing query:
      """
      FETCH PROP ON player hash("Tony Parker") YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    # check value by fetch
    When executing query:
      """
      FETCH PROP ON serve hash("Tony Parker")->hash("Spurs") YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |
    # after delete to check value by go
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst    |
      | "Tim Duncan" |
    # after delete to check value by go
    When executing query:
      """
      GO FROM hash("Tony Parker") OVER like REVERSELY
      """
    Then the result should be, in any order:
      | like._dst |
    # before delete multi vertexes to check value by go
    When executing query:
      """
      GO FROM hash("Chris Paul") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst         |
      | "LeBron James"    |
      | "Dwyane Wade"     |
      | "Carmelo Anthony" |
    # delete multi vertexes
    When executing query:
      """
      DELETE VERTEX hash("LeBron James"), hash("Dwyane Wade"), hash("Carmelo Anthony");
      """
    Then the execution should be successful
    # after delete multi vertexes to check value by go
    When executing query:
      """
      FETCH PROP ON player hash("Tony Parker") YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    # before delete hash id vertex to check value by go
    When executing query:
      """
      GO FROM hash("Tracy McGrady") OVER like
      """
    Then the result should be, in any order:
      | like._dst            |
      | 6293765385213992205  |
      | -2308681984240312228 |
      | -3212290852619976819 |
    # before delete hash id vertex to check value by go
    When executing query:
      """
      GO FROM hash("Grant Hill") OVER like REVERSELY
      """
    Then the result should be, in any order:
      | like._dst           |
      | 4823234394086728974 |
    # before delete hash id vertex to check value by fetch
    When executing query:
      """
      FETCH PROP ON player hash("Grant Hill") YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID            | player.name  | player.age |
      | 6293765385213992205 | "Grant Hill" | 46         |
    # before delete hash id vertex to check value by fetch
    When executing query:
      """
      FETCH PROP ON serve hash("Grant Hill")->hash("Pistons") YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src          | serve._dst           | serve._rank | serve.start_year | serve.end_year |
      | 6293765385213992205 | -2742277443392542725 | 0           | 1994             | 2000           |
    # delete hash id vertex
    When executing query:
      """
      DELETE VERTEX hash("Grant Hill")
      """
    Then the execution should be successful
    # after delete hash id vertex to check value by go
    When executing query:
      """
      GO FROM hash("Tracy McGrady") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst     |
      | "Kobe Bryant" |
      | "Rudy Gay"    |
    # after delete hash id vertex to check value by go
    When executing query:
      """
      GO FROM hash("Grant Hill") OVER like REVERSELY
      """
    Then the result should be, in any order:
      | like._dst |
    # after delete hash id vertex to check value by fetch
    When executing query:
      """
      FETCH PROP ON player hash("Grant Hill") YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    # delete not existed vertex
    When executing query:
      """
      DELETE VERTEX hash("Non-existing Vertex")
      """
    Then the execution should be successful
    # delete a vertex without edges
    When executing query:
      """
      INSERT VERTEX player(name, age) VALUES hash("A Loner"): ("A Loner", 0);
      DELETE VERTEX hash("A Loner");
      """
    Then the execution should be successful
    # check delete a vertex without edges
    When executing query:
      """
      FETCH PROP ON player hash("A Loner") YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    # delete with no edge
    When executing query:
      """
      DELETE VERTEX hash("Nobody")
      """
    Then the execution should be successful
    # check delete with no edge
    When executing query:
      """
      FETCH PROP ON player hash("Nobody") YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |

  Scenario: delete int vertex by pipe successed
    Given load "nba_int_vid" csv data to a new space
    # test delete with pipe wrong vid type
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like YIELD (string)like._src as id | DELETE VERTEX $-.id
      """
    Then a SemanticError should be raised at runtime:
    # delete with pipe, get result by go
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst     |
      | "Tony Parker" |
      | "Tim Duncan"  |
    When executing query:
      """
      GO FROM hash("Tony Parker") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst       |
      | "Tony Parker"   |
      | "Manu Ginobili" |
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like YIELD like._dst as id | DELETE VERTEX $-.id
      """
    Then the execution should be successful
    When executing query:
      """
      GO FROM hash("Boris Diaw") OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM hash("Tony Parker") OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like
      """
    Then the result should be, in any order:
      | like._dst |

  Scenario: delete with pipe failed, because of the wrong vid type
    When executing query:
      """
      USE nba_int_vid;YIELD "Tom" as id | DELETE VERTEX $-.id;
      """
    Then a SemanticError should be raised at runtime: The vid `$-.id' should be type of `INT', but was`STRING'
    Then drop the used space

  Scenario: delete with var, get result by go
    Given load "nba_int_vid" csv data to a new space
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst      |
      | "Paul George"  |
      | "James Harden" |
    When executing query:
      """
      GO FROM hash("Paul George") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst           |
      | "Russell Westbrook" |
    When executing query:
      """
      GO FROM hash("James Harden") OVER like
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | like._dst           |
      | "Russell Westbrook" |
    When executing query:
      """
      $var = GO FROM hash("Russell Westbrook") OVER like YIELD like._dst as id; DELETE VERTEX $var.id
      """
    Then the execution should be successful
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM hash("Paul George") OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM hash("Russell Westbrook") OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    Then drop the used space

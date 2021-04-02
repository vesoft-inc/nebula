# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Delete string vid of vertex

  Scenario: delete string vertex
    Given load "nba" csv data to a new space
    # get vertex info
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like
      """
    Then the result should be, in any order:
      | like._dst     |
      | "Tony Parker" |
      | "Tim Duncan"  |
    When executing query:
      """
      GO FROM "Tony Parker" OVER like REVERSELY
      """
    Then the result should be, in any order:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Boris Diaw"        |
      | "Dejounte Murray"   |
      | "Marco Belinelli"   |
      | "Tim Duncan"        |
    # get value by fetch
    When executing query:
      """
      FETCH PROP ON player "Tony Parker" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
    # check value by fetch
    When executing query:
      """
      FETCH PROP ON serve "Tony Parker"->"Spurs" YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src    | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Tony Parker" | "Spurs"    | 0           | 1999             | 2018           |
    # delete one vertex
    When executing query:
      """
      DELETE VERTEX "Tony Parker";
      """
    Then the execution should be successful
    # after delete to check value by fetch
    When executing query:
      """
      FETCH PROP ON player "Tony Parker" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    # check value by fetch
    When executing query:
      """
      FETCH PROP ON serve "Tony Parker"->"Spurs" YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src | serve._dst | serve._rank | serve.start_year | serve.end_year |
    # after delete to check value by go
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like
      """
    Then the result should be, in any order:
      | like._dst    |
      | "Tim Duncan" |
    # after delete to check value by go
    When executing query:
      """
      GO FROM "Tony Parker" OVER like REVERSELY
      """
    Then the result should be, in any order:
      | like._dst |
    # before delete multi vertexes to check value by go
    When executing query:
      """
      GO FROM "Chris Paul" OVER like
      """
    Then the result should be, in any order:
      | like._dst         |
      | "LeBron James"    |
      | "Dwyane Wade"     |
      | "Carmelo Anthony" |
    # delete multi vertexes
    When executing query:
      """
      DELETE VERTEX "LeBron James", "Dwyane Wade", "Carmelo Anthony";
      """
    Then the execution should be successful
    # after delete multi vertexes to check value by go
    When executing query:
      """
      FETCH PROP ON player "Tony Parker" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    # before delete hash id vertex to check value by go
    When executing query:
      """
      GO FROM "Tracy McGrady" OVER like
      """
    Then the result should be, in any order:
      | like._dst     |
      | "Kobe Bryant" |
      | "Grant Hill"  |
      | -"Rudy Gay"   |
    # before delete hash id vertex to check value by go
    When executing query:
      """
      GO FROM "Grant Hill" OVER like REVERSELY
      """
    Then the result should be, in any order:
      | like._dst       |
      | "Tracy McGrady" |
    # before delete hash id vertex to check value by fetch
    When executing query:
      """
      FETCH PROP ON player "Grant Hill" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Grant Hill" | "Grant Hill" | 46         |
    # before delete hash id vertex to check value by fetch
    When executing query:
      """
      FETCH PROP ON serve "Grant Hill"->"Pistons" YIELD serve.start_year, serve.end_year
      """
    Then the result should be, in any order:
      | serve._src   | serve._dst | serve._rank | serve.start_year | serve.end_year |
      | "Grant Hill" | "Pistons"  | 0           | 1994             | 2000           |
    # delete hash id vertex
    When executing query:
      """
      DELETE VERTEX "Grant Hill"
      """
    Then the execution should be successful
    # after delete hash id vertex to check value by go
    When executing query:
      """
      GO FROM "Tracy McGrady" OVER like
      """
    Then the result should be, in any order:
      | like._dst     |
      | "Kobe Bryant" |
      | "Rudy Gay"    |
    # after delete hash id vertex to check value by go
    When executing query:
      """
      GO FROM "Grant Hill" OVER like REVERSELY
      """
    Then the result should be, in any order:
      | like._dst |
    # after delete hash id vertex to check value by fetch
    When executing query:
      """
      FETCH PROP ON player "Grant Hill" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    # delete not existed vertex
    When executing query:
      """
      DELETE VERTEX "Non-existing Vertex"
      """
    Then the execution should be successful
    # delete a vertex without edges
    When executing query:
      """
      INSERT VERTEX player(name, age) VALUES "A Loner": ("A Loner", 0);
      DELETE VERTEX "A Loner";
      """
    Then the execution should be successful
    # check delete a vertex without edges
    When executing query:
      """
      FETCH PROP ON player "A Loner" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    # delete with no edge
    When executing query:
      """
      DELETE VERTEX "Nobody"
      """
    Then the execution should be successful
    # check delete with no edge
    When executing query:
      """
      FETCH PROP ON player "Nobody" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID | player.name | player.age |
    Then drop the used space

  Scenario: delete string vertex by pipe
    Given load "nba" csv data to a new space
    # test delete with pipe wrong vid type
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like YIELD like._type as id | DELETE VERTEX $-.id
      """
    Then a SemanticError should be raised at runtime:
    # delete with pipe, get result by go
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like
      """
    Then the result should be, in any order:
      | like._dst     |
      | "Tony Parker" |
      | "Tim Duncan"  |
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      """
    Then the result should be, in any order:
      | like._dst           |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like
      """
    Then the result should be, in any order:
      | like._dst       |
      | "Tony Parker"   |
      | "Manu Ginobili" |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like YIELD like._dst as id | DELETE VERTEX $-.id
      """
    Then the execution should be successful
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like
      """
    Then the result should be, in any order:
      | like._dst |

  Scenario: delete with var, get result by go
    Given load "nba" csv data to a new space
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER like
      """
    Then the result should be, in any order:
      | like._dst      |
      | "Paul George"  |
      | "James Harden" |
    When executing query:
      """
      GO FROM "Paul George" OVER like
      """
    Then the result should be, in any order:
      | like._dst           |
      | "Russell Westbrook" |
    When executing query:
      """
      GO FROM "James Harden" OVER like
      """
    Then the result should be, in any order:
      | like._dst           |
      | "Russell Westbrook" |
    When executing query:
      """
      $var = GO FROM "Russell Westbrook" OVER like YIELD like._dst as id; DELETE VERTEX $var.id
      """
    Then the execution should be successful
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM "Paul George" OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    Then drop the used space

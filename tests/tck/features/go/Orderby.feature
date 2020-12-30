# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Orderby Sentence

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: Syntax Error
    When executing query:
      """
      ORDER BY
      """
    Then a SyntaxError should be raised at runtime:
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name | ORDER BY $-.$$.team.name
      """
    Then a SyntaxError should be raised at runtime:
    When executing query:
      """
      ORDER BY $-.xx
      """
    Then a SyntaxError should be raised at runtime:

  Scenario: Empty Input
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team| ORDER BY $-.name
      """
    Then the result should be, in order, with relax comparison:
      | name | start | team |
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team
      | YIELD $-.name as name WHERE $-.start > 20000
      | ORDER BY $-.name
      """
    Then the result should be, in order, with relax comparison:
      | name |

  Scenario: Orderby not existing prop
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | ORDER BY $-.abc
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Sorting results using single column
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | ORDER BY $-.team
      """
    Then the result should be, in order, with relax comparison:
      | name         | start | team      |
      | "Boris Diaw" | 2003  | "Hawks"   |
      | "Boris Diaw" | 2008  | "Hornets" |
      | "Boris Diaw" | 2016  | "Jazz"    |
      | "Boris Diaw" | 2012  | "Spurs"   |
      | "Boris Diaw" | 2005  | "Suns"    |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | ORDER BY $-.team ASC
      """
    Then the result should be, in order, with relax comparison:
      | name         | start | team      |
      | "Boris Diaw" | 2003  | "Hawks"   |
      | "Boris Diaw" | 2008  | "Hornets" |
      | "Boris Diaw" | 2016  | "Jazz"    |
      | "Boris Diaw" | 2012  | "Spurs"   |
      | "Boris Diaw" | 2005  | "Suns"    |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team | ORDER BY $-.team DESC
      """
    Then the result should be, in order, with relax comparison:
      | name         | start | team      |
      | "Boris Diaw" | 2005  | "Suns"    |
      | "Boris Diaw" | 2012  | "Spurs"   |
      | "Boris Diaw" | 2016  | "Jazz"    |
      | "Boris Diaw" | 2008  | "Hornets" |
      | "Boris Diaw" | 2003  | "Hawks"   |

  Scenario: Sorting using multi columns
    When executing query:
      """
      GO FROM "Boris Diaw","LaMarcus Aldridge" OVER serve WHERE serve.start_year >= 2012 YIELD $$.team.name as team, $^.player.name as player,
      $^.player.age as age, serve.start_year as start | ORDER BY $-.team, $-.age
      """
    Then the result should be, in order, with relax comparison:
      | team    | player              | age | start |
      | "Jazz"  | "Boris Diaw"        | 36  | 2016  |
      | "Spurs" | "LaMarcus Aldridge" | 33  | 2015  |
      | "Spurs" | "Boris Diaw"        | 36  | 2012  |
    When executing query:
      """
      GO FROM "Boris Diaw","LaMarcus Aldridge" OVER serve WHERE serve.start_year >= 2012 YIELD $$.team.name as team, $^.player.name as player,
      $^.player.age as age, serve.start_year as start | ORDER BY $-.team, $-.age ASC
      """
    Then the result should be, in order, with relax comparison:
      | team    | player              | age | start |
      | "Jazz"  | "Boris Diaw"        | 36  | 2016  |
      | "Spurs" | "LaMarcus Aldridge" | 33  | 2015  |
      | "Spurs" | "Boris Diaw"        | 36  | 2012  |
    When executing query:
      """
      GO FROM "Boris Diaw","LaMarcus Aldridge" OVER serve WHERE serve.start_year >= 2012 YIELD $$.team.name as team, $^.player.name as player,
      $^.player.age as age, serve.start_year as start | ORDER BY $-.team, $-.age DESC
      """
    Then the result should be, in order, with relax comparison:
      | team    | player              | age | start |
      | "Jazz"  | "Boris Diaw"        | 36  | 2016  |
      | "Spurs" | "Boris Diaw"        | 36  | 2012  |
      | "Spurs" | "LaMarcus Aldridge" | 33  | 2015  |
    When executing query:
      """
      GO FROM "Boris Diaw","LaMarcus Aldridge" OVER serve WHERE serve.start_year >= 2012 YIELD $$.team.name as team, $^.player.name as player,
      $^.player.age as age, serve.start_year as start | ORDER BY $-.team DESC, $-.age ASC
      """
    Then the result should be, in order, with relax comparison:
      | team    | player              | age | start |
      | "Spurs" | "LaMarcus Aldridge" | 33  | 2015  |
      | "Spurs" | "Boris Diaw"        | 36  | 2012  |
      | "Jazz"  | "Boris Diaw"        | 36  | 2016  |
    When executing query:
      """
      GO FROM "Boris Diaw","LaMarcus Aldridge" OVER serve WHERE serve.start_year >= 2012 YIELD $$.team.name as team, $^.player.name as player,
      $^.player.age as age, serve.start_year as start | ORDER BY $-.team DESC, $-.age DESC
      """
    Then the result should be, in order, with relax comparison:
      | team    | player              | age | start |
      | "Spurs" | "Boris Diaw"        | 36  | 2012  |
      | "Spurs" | "LaMarcus Aldridge" | 33  | 2015  |
      | "Jazz"  | "Boris Diaw"        | 36  | 2016  |
    When executing query:
      """
      GO FROM "Boris Diaw", "LaMarcus Aldridge" OVER serve WHERE serve.start_year >= 2012 YIELD $$.team.name as team, $^.player.name as player,
      $^.player.age as age, serve.start_year as start | ORDER BY $-.team ASC, $-.age ASC
      """
    Then the result should be, in order, with relax comparison:
      | team    | player              | age | start |
      | "Jazz"  | "Boris Diaw"        | 36  | 2016  |
      | "Spurs" | "LaMarcus Aldridge" | 33  | 2015  |
      | "Spurs" | "Boris Diaw"        | 36  | 2012  |
    When executing query:
      """
      GO FROM "Boris Diaw", "LaMarcus Aldridge" OVER serve WHERE serve.start_year >= 2012 YIELD $$.team.name as team, $^.player.name as player,
      $^.player.age as age, serve.start_year as start | ORDER BY $-.team ASC, $-.age DESC
      """
    Then the result should be, in order, with relax comparison:
      | team    | player              | age | start |
      | "Jazz"  | "Boris Diaw"        | 36  | 2016  |
      | "Spurs" | "Boris Diaw"        | 36  | 2012  |
      | "Spurs" | "LaMarcus Aldridge" | 33  | 2015  |

  Scenario: Pipe output of ORDER BY to graph traversal
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like YIELD like._dst as id | ORDER BY $-.id | GO FROM $-.id over serve
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst |
      | "Spurs"    |
      | "Spurs"    |
      | "Hornets"  |

  Scenario: Duplicate columns
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve YIELD $^.player.name as team, serve.start_year as start, $$.team.name as team | ORDER BY $-.team
      """
    Then a SemanticError should be raised at runtime:

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Set Test

  Scenario: Union All
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | $$.team.name |
      | "Tim Duncan"   | 1997             | "Spurs"      |
      | "Tony Parker"  | 1999             | "Spurs"      |
      | "Tony Parker"  | 2018             | "Hornets"    |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM "Manu Ginobili" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Tim Duncan"    | 1997             | "Spurs"      |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |
      | "Manu Ginobili" | 2002             | "Spurs"      |
    When executing query:
      """
      (GO FROM "Tim Duncan" OVER like YIELD like._dst AS id |
          GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      UNION ALL
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      (GO FROM "Tony Parker" OVER like YIELD like._dst AS id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      """
    Then the result should be, in any order:
      | $^.player.name      | serve.start_year | $$.team.name    |
      | "Tim Duncan"        | 1997             | "Spurs"         |
      | "LaMarcus Aldridge" | 2015             | "Spurs"         |
      | "LaMarcus Aldridge" | 2006             | "Trail Blazers" |
      | "Manu Ginobili"     | 2002             | "Spurs"         |
      | "Tim Duncan"        | 1997             | "Spurs"         |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM "Tony Parker" OVER like YIELD like._dst AS id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name      | serve.start_year | $$.team.name    |
      | "Tim Duncan"        | 1997             | "Spurs"         |
      | "LaMarcus Aldridge" | 2015             | "Spurs"         |
      | "LaMarcus Aldridge" | 2006             | "Trail Blazers" |
      | "Manu Ginobili"     | 2002             | "Spurs"         |
      | "Tim Duncan"        | 1997             | "Spurs"         |
    When executing query:
      """
      (GO FROM "Tim Duncan" OVER like YIELD like._dst AS id
       UNION ALL
       GO FROM "Tony Parker" OVER like YIELD like._dst AS id)
      | GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name      | serve.start_year | $$.team.name    |
      | "Manu Ginobili"     | 2002             | "Spurs"         |
      | "Manu Ginobili"     | 2002             | "Spurs"         |
      | "Tony Parker"       | 1999             | "Spurs"         |
      | "Tony Parker"       | 2018             | "Hornets"       |
      | "LaMarcus Aldridge" | 2015             | "Spurs"         |
      | "LaMarcus Aldridge" | 2006             | "Trail Blazers" |
      | "Tim Duncan"        | 1997             | "Spurs"         |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD $^.player.name as name, $$.team.name as player
      UNION ALL
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name as name, serve.start_year as player
      """
    Then the result should be, in any order:
      | name          | player  |
      | "Tim Duncan"  | "Spurs" |
      | "Tony Parker" | 1999    |
      | "Tony Parker" | 2018    |
    When executing query:
      """
      GO FROM "Nobody" OVER serve YIELD $^.player.name AS player, serve.start_year AS start
      UNION ALL
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name AS player, serve.start_year AS start
      """
    Then the result should be, in any order:
      | player        | start |
      | "Tony Parker" | 1999  |
      | "Tony Parker" | 2018  |

  Scenario: Union Distinct
    Given a graph with space named "nba"
    When executing query:
      """
      (GO FROM "Tim Duncan" OVER like YIELD like._dst as id |
          GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      UNION
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION
      GO FROM "Manu Ginobili" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |
    When executing query:
      """
      (GO FROM "Tim Duncan" OVER like YIELD like._dst as id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      UNION DISTINCT
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |

  Scenario: Minus
    Given a graph with space named "nba"
    When executing query:
      """
      (GO FROM "Tim Duncan" OVER like YIELD like._dst as id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      MINUS
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |

  Scenario: Intersect
    Given a graph with space named "nba"
    When executing query:
      """
      (GO FROM "Tim Duncan" OVER like YIELD like._dst as id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      INTERSECT
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | $$.team.name |
      | "Tony Parker"  | 1999             | "Spurs"      |
      | "Tony Parker"  | 2018             | "Hornets"    |

  Scenario: Mix
    Given a graph with space named "nba"
    When executing query:
      """
      (GO FROM "Tim Duncan" OVER like YIELD like._dst as id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      MINUS
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION
      GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      INTERSECT
      GO FROM "Manu Ginobili" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |

  Scenario: Assign
    Given a graph with space named "nba"
    When executing query:
      """
      $var = GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.$^.player.name | $var.serve.start_year | $var.$$.team.name |
      | "Tim Duncan"        | 1997                  | "Spurs"           |
      | "Tony Parker"       | 1999                  | "Spurs"           |
      | "Tony Parker"       | 2018                  | "Hornets"         |
    When executing query:
      """
      $var = (GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name);
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.$^.player.name | $var.serve.start_year | $var.$$.team.name |
      | "Tim Duncan"        | 1997                  | "Spurs"           |
      | "Tony Parker"       | 1999                  | "Spurs"           |
      | "Tony Parker"       | 2018                  | "Hornets"         |
    When executing query:
      """
      $var = (GO FROM "Tim Duncan" OVER like YIELD like._dst as id |
      GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      MINUS
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.$^.player.name | $var.serve.start_year | $var.$$.team.name |
      | "Manu Ginobili"     | 2002                  | "Spurs"           |
    When executing query:
      """
      $var = (GO FROM "Tim Duncan" OVER like YIELD like._dst as id |
           GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      INTERSECT
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.$^.player.name | $var.serve.start_year | $var.$$.team.name |
      | "Tony Parker"       | 1999                  | "Spurs"           |
      | "Tony Parker"       | 2018                  | "Hornets"         |

  Scenario: Empty Input
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name
      UNION
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name
      MINUS
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name
      INTERSECT
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | serve.start_year | $$.team.name |
    When executing query:
      """
      $var = GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name
      UNION
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name
      MINUS
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name
      INTERSECT
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD serve.start_year, $$.team.name;
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.serve.start_year | $var.$$.team.name |

  Scenario: Diffrent column
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD $^.player.name as name, $$.team.name as player
      UNION ALL
      GO FROM "Tony Parker" OVER serve
      YIELD $^.player.name as name, serve.start_year
      """
    Then a SemanticError should be raised at runtime: different column names to UNION/INTERSECT/MINUS are not supported

  Scenario: Pipe to a set op
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "123" OVER like YIELD like._src as src, like._dst as dst
      | (GO FROM $-.src OVER serve UNION GO FROM $-.dst OVER serve)
      """
    Then a SemanticError should be raised at runtime: `$-.src', not exist prop `src'

  Scenario: Non-existent props
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION
      GO FROM "Tony Parker" OVER serve YIELD $^.player.name1, serve.start_year, $$.team.name
      """
    Then a SemanticError should be raised at runtime: `$^.player.name1', not found the property `name1'

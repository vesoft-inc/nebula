# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Integer Vid Set Test

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Integer Vid Union All
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | $$.team.name |
      | "Tim Duncan"   | 1997             | "Spurs"      |
      | "Tony Parker"  | 1999             | "Spurs"      |
      | "Tony Parker"  | 2018             | "Hornets"    |
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM hash("Manu Ginobili") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Tim Duncan"    | 1997             | "Spurs"      |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |
      | "Manu Ginobili" | 2002             | "Spurs"      |
    When executing query:
      """
      (GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS id |
          GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      UNION ALL
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
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
      GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
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
      GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      (GO FROM hash("Tony Parker") OVER like YIELD like._dst AS id |
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
      GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM hash("Tony Parker") OVER like YIELD like._dst AS id |
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
      (GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS id
       UNION ALL
       GO FROM hash("Tony Parker") OVER like YIELD like._dst AS id)
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
      GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name as name, $$.team.name as player
      UNION ALL
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name as name, serve.start_year as player
      """
    Then the result should be, in any order:
      | name          | player  |
      | "Tim Duncan"  | "Spurs" |
      | "Tony Parker" | 1999    |
      | "Tony Parker" | 2018    |
    When executing query:
      """
      GO FROM hash("Nobody") OVER serve YIELD $^.player.name AS player, serve.start_year AS start
      UNION ALL
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name AS player, serve.start_year AS start
      """
    Then the result should be, in any order:
      | player        | start |
      | "Tony Parker" | 1999  |
      | "Tony Parker" | 2018  |

  Scenario: Integer Vid Union Distinct
    When executing query:
      """
      (GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id |
          GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      UNION
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION
      GO FROM hash("Manu Ginobili") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |
    When executing query:
      """
      (GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      UNION DISTINCT
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |
      | "Tony Parker"   | 1999             | "Spurs"      |
      | "Tony Parker"   | 2018             | "Hornets"    |

  Scenario: Integer Vid Minus
    When executing query:
      """
      (GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      MINUS
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |

  Scenario: Integer Vid Intersect
    When executing query:
      """
      (GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      INTERSECT
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | $$.team.name |
      | "Tony Parker"  | 1999             | "Spurs"      |
      | "Tony Parker"  | 2018             | "Hornets"    |

  Scenario: Integer Vid Mix
    When executing query:
      """
      (GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id |
         GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      MINUS
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION
      GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      INTERSECT
      GO FROM hash("Manu Ginobili") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name  | serve.start_year | $$.team.name |
      | "Manu Ginobili" | 2002             | "Spurs"      |

  Scenario: Integer Vid Assign
    When executing query:
      """
      $var = GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION ALL
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.$^.player.name | $var.serve.start_year | $var.$$.team.name |
      | "Tim Duncan"        | 1997                  | "Spurs"           |
      | "Tony Parker"       | 1999                  | "Spurs"           |
      | "Tony Parker"       | 2018                  | "Hornets"         |
    When executing query:
      """
      $var = (GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name);
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.$^.player.name | $var.serve.start_year | $var.$$.team.name |
      | "Tim Duncan"        | 1997                  | "Spurs"           |
      | "Tony Parker"       | 1999                  | "Spurs"           |
      | "Tony Parker"       | 2018                  | "Hornets"         |
    When executing query:
      """
      $var = (GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id |
      GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      MINUS
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.$^.player.name | $var.serve.start_year | $var.$$.team.name |
      | "Manu Ginobili"     | 2002                  | "Spurs"           |
    When executing query:
      """
      $var = (GO FROM hash("Tim Duncan") OVER like YIELD like._dst as id |
           GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)
      INTERSECT
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.$^.player.name | $var.serve.start_year | $var.$$.team.name |
      | "Tony Parker"       | 1999                  | "Spurs"           |
      | "Tony Parker"       | 2018                  | "Hornets"         |

  Scenario: Integer Vid Empty Input
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve.start_year, $$.team.name
      UNION
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve.start_year, $$.team.name
      MINUS
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve.start_year, $$.team.name
      INTERSECT
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve.start_year, $$.team.name
      """
    Then the result should be, in any order:
      | serve.start_year | $$.team.name |
    When executing query:
      """
      $var = GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve.start_year, $$.team.name
      UNION
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve.start_year, $$.team.name
      MINUS
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve.start_year, $$.team.name
      INTERSECT
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD serve.start_year, $$.team.name;
      YIELD $var.*
      """
    Then the result should be, in any order:
      | $var.serve.start_year | $var.$$.team.name |

  Scenario: Integer Vid Diffrent column
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name as name, $$.team.name as player
      UNION ALL
      GO FROM hash("Tony Parker") OVER serve
      YIELD $^.player.name as name, serve.start_year
      """
    Then a SemanticError should be raised at runtime: different column names to UNION/INTERSECT/MINUS are not supported

  Scenario: Integer Vid Pipe to a set op
    When executing query:
      """
      GO FROM 123 OVER like YIELD like._src as src, like._dst as dst
      | (GO FROM $-.src OVER serve YIELD serve._dst UNION GO FROM $-.dst OVER serve YIELD serve._dst)
      """
    Then a SemanticError should be raised at runtime: `$-.src', not exist prop `src'

  Scenario: Integer Vid Non-existent props
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve YIELD $^.player.name, serve.start_year, $$.team.name
      UNION
      GO FROM hash("Tony Parker") OVER serve YIELD $^.player.name1, serve.start_year, $$.team.name
      """
    Then a SemanticError should be raised at runtime: `$^.player.name1', not found the property `name1'

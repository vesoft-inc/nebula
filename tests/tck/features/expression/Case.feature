# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Case Expression

  Background:
    Given a graph with space named "nba"

  Scenario: yield generic case
    When executing query:
      """
      YIELD CASE 2 + 3 WHEN 4 THEN 0 WHEN 5 THEN 1 ELSE 2 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 1 |
    When executing query:
      """
      YIELD CASE true WHEN false THEN 0 END AS r
      """
    Then the result should be, in any order:
      | r    |
      | NULL |
    When executing query:
      """
      YIELD CASE WHEN 4 > 5 THEN 0 WHEN 3+4==7 THEN 1 ELSE 2 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 1 |
    When executing query:
      """
      YIELD CASE WHEN false THEN 0 ELSE 1 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 1 |

  Scenario: yield conditional case
    When executing query:
      """
      YIELD 3 > 5 ? 0 : 1 AS r
      """
    Then the result should be, in any order:
      | r |
      | 1 |
    When executing query:
      """
      YIELD true ? "yes" : "no" AS r
      """
    Then the result should be, in any order:
      | r     |
      | "yes" |

  Scenario: use generic case in GO
    When executing query:
      """
      GO FROM "Jonathon Simmons" OVER serve
      YIELD $$.team.name AS name,
            CASE serve.end_year > 2017 WHEN true THEN "ok" ELSE "no" END AS b
      """
    Then the result should be, in any order:
      | name    | b    |
      | 'Spurs' | 'no' |
      | 'Magic' | 'ok' |
      | '76ers' | 'ok' |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve
      YIELD $^.player.name AS player_name,
            serve.start_year AS start_year,
            serve.end_year AS end_year,
            CASE serve.start_year > 2006 WHEN true THEN "new" ELSE "old" END AS b,
            $$.team.name AS team_name
      """
    Then the result should be, in any order:
      | player_name  | start_year | end_year | b     | team_name |
      | "Boris Diaw" | 2003       | 2005     | "old" | "Hawks"   |
      | "Boris Diaw" | 2005       | 2008     | "old" | "Suns"    |
      | "Boris Diaw" | 2008       | 2012     | "new" | "Hornets" |
      | "Boris Diaw" | 2012       | 2016     | "new" | "Spurs"   |
      | "Boris Diaw" | 2016       | 2017     | "new" | "Jazz"    |
    When executing query:
      """
      GO FROM "Rajon Rondo" OVER serve
      WHERE CASE serve.start_year WHEN 2016 THEN true ELSE false END
      YIELD $^.player.name AS player_name,
            serve.start_year AS start_year,
            serve.end_year AS end_year,
            $$.team.name AS team_name
      """
    Then the result should be, in any order:
      | player_name   | start_year | end_year | team_name |
      | "Rajon Rondo" | 2016       | 2017     | "Bulls"   |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve
      YIELD $$.team.name AS name,
            CASE WHEN serve.start_year < 1998 THEN "old" ELSE "young" END AS b
      """
    Then the result should be, in any order:
      | name    | b     |
      | 'Spurs' | 'old' |
    When executing query:
      """
      GO FROM "Rajon Rondo" OVER serve
      WHERE CASE WHEN serve.start_year > 2016 THEN true ELSE false END
      YIELD $^.player.name AS player_name,
            serve.start_year AS start_year,
            serve.end_year AS end_year,
            $$.team.name AS team_name
      """
    Then the result should be, in any order:
      | player_name   | start_year | end_year | team_name  |
      | "Rajon Rondo" | 2018       | 2019     | "Lakers"   |
      | "Rajon Rondo" | 2017       | 2018     | "Pelicans" |

  Scenario: use conditional case in GO
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve
      YIELD $$.team.name AS name,
            serve.start_year < 1998 ? "old" : "young" AS b
      """
    Then the result should be, in any order:
      | name    | b     |
      | 'Spurs' | 'old' |
    When executing query:
      """
      GO FROM "Rajon Rondo" OVER serve
      WHERE serve.start_year > 2016 ? true : false
      YIELD $^.player.name AS player_name,
            serve.start_year AS start_year,
            serve.end_year AS end_year,
            $$.team.name AS team_name
      """
    Then the result should be, in any order:
      | player_name   | start_year | end_year | team_name  |
      | "Rajon Rondo" | 2018       | 2019     | "Lakers"   |
      | "Rajon Rondo" | 2017       | 2018     | "Pelicans" |

  Scenario: use case in MATCH
    When executing query:
      """
      MATCH (v:player) WHERE CASE v.age > 45 WHEN false THEN false ELSE true END
      RETURN v.name, v.age
      """
    Then the result should be, in any order:
      | v.name            | v.age |
      | "Shaquile O'Neal" | 47    |
      | "Grant Hill"      | 46    |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.age > 43
      RETURN CASE WHEN v.age > 46 THEN v.name WHEN v.age > 45 THEN v.age ELSE "nothing" END AS r
      """
    Then the result should be, in any order:
      | r                 |
      | "nothing"         |
      | 46                |
      | "Shaquile O'Neal" |
      | "nothing"         |

  Scenario: mixed use of generic case and conditional case
    When executing query:
      """
      YIELD CASE 2 + 3
            WHEN CASE 1 WHEN 1 THEN 5 ELSE 4 END THEN 0
            WHEN 5 THEN 1
            ELSE 2
            END AS r
      """
    Then the result should be, in any order:
      | r |
      | 0 |
    When executing query:
      """
      YIELD CASE 2 + 3
            WHEN 5 THEN CASE 1 WHEN 1 THEN 7 ELSE 4 END
            ELSE 2
            END AS r
      """
    Then the result should be, in any order:
      | r |
      | 7 |
    When executing query:
      """
      YIELD CASE 2 + 3
            WHEN 3 THEN 7
            ELSE CASE 9 WHEN 8 THEN 10 ELSE 11 END
            END AS r
      """
    Then the result should be, in any order:
      | r  |
      | 11 |
    When executing query:
      """
      YIELD CASE 3 > 2 ? 1 : 0 WHEN 1 THEN 5 ELSE 4 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 5 |
    When executing query:
      """
      YIELD CASE 1 WHEN true ? 1 : 0 THEN 5 ELSE 4 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 5 |
    When executing query:
      """
      YIELD CASE 1 WHEN 1 THEN 7 > 0 ? 6 : 9 ELSE 4 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 6 |
    When executing query:
      """
      YIELD CASE 1 WHEN 2 THEN 6 ELSE false ? 4 : 9 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 9 |
    When executing query:
      """
      YIELD CASE WHEN 2 > 7
            THEN false ? 3 : 8
            ELSE CASE true WHEN false THEN 9 ELSE 11 END
            END AS r
      """
    Then the result should be, in any order:
      | r  |
      | 11 |
    When executing query:
      """
      YIELD CASE 3 WHEN 4 THEN 5 ELSE 6 END > 11 ? 7 : CASE WHEN true THEN 8 ELSE 9 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 8 |
    When executing query:
      """
      YIELD 8 > 11 ? CASE WHEN true THEN 8 ELSE 9 END : CASE 14 WHEN 8+6 THEN 0 ELSE 1 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 0 |
    When executing query:
      """
      YIELD CASE 3 WHEN 4 THEN 5 ELSE 6 END > (3 > 2 ? 8 : 9) ?
            CASE WHEN true THEN 8 ELSE 9 END :
            CASE 14 WHEN 8+6 THEN 0 ELSE 1 END AS r
      """
    Then the result should be, in any order:
      | r |
      | 0 |
    When executing query:
      """
      GO FROM "Jonathon Simmons" OVER serve
      YIELD $$.team.name AS name,
            CASE serve.end_year > 2017
            WHEN true THEN 2017 < 2020 ? "ok" : "no"
            ELSE CASE WHEN false THEN "good" ELSE "bad" END
            END AS b
      """
    Then the result should be, in any order:
      | name    | b     |
      | 'Spurs' | 'bad' |
      | 'Magic' | 'ok'  |
      | '76ers' | 'ok'  |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve
      YIELD $^.player.name AS player_name,
            serve.start_year AS start_year,
            serve.end_year AS end_year,
            CASE serve.start_year > 2006 ? false : true
            WHEN true THEN "new" ELSE CASE WHEN serve.start_year != 2012 THEN "old"
            WHEN serve.start_year > 2009 THEN "bad" ELSE "good" END END AS b,
            $$.team.name AS team_name
      """
    Then the result should be, in any order:
      | player_name  | start_year | end_year | b     | team_name |
      | "Boris Diaw" | 2003       | 2005     | "new" | "Hawks"   |
      | "Boris Diaw" | 2005       | 2008     | "new" | "Suns"    |
      | "Boris Diaw" | 2008       | 2012     | "old" | "Hornets" |
      | "Boris Diaw" | 2012       | 2016     | "bad" | "Spurs"   |
      | "Boris Diaw" | 2016       | 2017     | "old" | "Jazz"    |

  Scenario: Using the return value of case expr as an input
    When executing query:
      """
      RETURN CASE WHEN true THEN "Tim Duncan" ELSE "ABC" END AS a | GO FROM $-.a OVER like;
      """
    Then the result should be, in order:
      | like._dst       |
      | "Manu Ginobili" |
      | "Tony Parker"   |

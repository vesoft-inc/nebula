Feature: Groupby & limit Sentence

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: Syntax test2
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name | GROUP BY 1+1 YIELD COUNT(1), 1+1
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Syntax test3
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name | GROUP BY $-.start_year YIELD COUNT($var)
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Syntax test4
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve.end_year AS end_year | GROUP BY $-.start_year YIELD COUNT($$.team.name)
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Syntax test5
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve._dst AS id | GROUP BY $-.start_year YIELD COUNT($-.id)
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Syntax test6
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve._dst AS id | GROUP BY team YIELD COUNT($-.id), $-.name AS teamName
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Syntax test7
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve._dst AS id | GROUP BY $-.name YIELD COUNT($-.start_year)
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Syntax test8
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve._dst AS id | GROUP BY $-.name YIELD SUM(*)
      """
    Then a SyntaxError should be raised at runtime:

  Scenario: Syntax test9
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve._dst AS id | GROUP BY $-.name YIELD COUNT($-.name, $-.id)
      """
    Then a SyntaxError should be raised at runtime:

  Scenario: Syntax test10
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve._dst AS id |  GROUP BY $-.name, SUM($-.id) YIELD $-.name,  SUM($-.id)
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Syntax test11
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, COUNT(serve._dst) AS id
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Limit test
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name | ORDER BY $-.name | LIMIT 5
      """
    Then the result should be, in order, with relax comparison:
      | name      |
      | "76ers"   |
      | "Bulls"   |
      | "Hawks"   |
      | "Hornets" |
      | "Hornets" |
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name | ORDER BY $-.name | LIMIT 2,2
      """
    Then the result should be, in order, with relax comparison:
      | name      |
      | "Hawks"   |
      | "Hornets" |
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name | ORDER BY $-.name | LIMIT 2 OFFSET 2
      """
    Then the result should be, in order, with relax comparison:
      | name      |
      | "Hawks"   |
      | "Hornets" |
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER like YIELD $$.team.name AS name, like._dst AS id | ORDER BY $-.name | LIMIT 1
      | GO FROM $-.id OVER like YIELD $$.player.name AS name | ORDER BY $-.name | LIMIT 2
      """
    Then the result should be, in order, with relax comparison:
      | name              |
      | "LeBron James"    |
      | "Marco Belinelli" |
    When executing query:
      """
      GO FROM hash("Danny Green") OVER serve YIELD $$.team.name AS name | LIMIT 5
      """
    Then the result should be, in any order, with relax comparison:
      | name        |
      | "Cavaliers" |
      | "Spurs"     |
      | "Raptors"   |
    When executing query:
      """
      GO FROM hash("Danny Green") OVER serve YIELD $$.team.name AS name | LIMIT 3, 2
      """
    Then the result should be, in any order, with relax comparison:
      | name |

  Scenario: OFFSET 0
    When executing query:
      """
      GO FROM hash("Danny Green") OVER serve YIELD $$.team.name AS name | LIMIT 1 OFFSET 0
      """
    Then the result should be, in any order, with relax comparison:
      | name        |
      | "Cavaliers" |

  Scenario: Groupby test
    When executing query:
      """
      GO FROM hash('Aron Baynes'), hash('Tracy McGrady') OVER serve
      YIELD $$.team.name AS name, serve._dst AS id, serve.start_year AS start_year, serve.end_year AS end_year
      | GROUP BY $-.name, $-.start_year
      YIELD $-.name AS teamName, $-.start_year AS start_year, MAX($-.start_year), MIN($-.end_year), AVG($-.end_year) AS avg_end_year,
      STD($-.end_year) AS std_end_year, COUNT($-.id)
      """
    Then the result should be, in any order:
      | teamName  | start_year | MAX($-.start_year) | MIN($-.end_year) | avg_end_year | std_end_year | COUNT($-.id) |
      | "Magic"   | 2000       | 2000               | 2004             | 2004.0       | 0.0          | 1            |
      | "Raptors" | 1997       | 1997               | 2000             | 2000.0       | 0.0          | 1            |
      | "Rockets" | 2004       | 2004               | 2010             | 2010.0       | 0.0          | 1            |
      | "Pistons" | 2015       | 2015               | 2017             | 2017.0       | 0.0          | 1            |
      | "Spurs"   | 2013       | 2013               | 2013             | 2014.0       | 1.0          | 2            |
      | "Celtics" | 2017       | 2017               | 2019             | 2019.0       | 0.0          | 1            |
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve._dst AS id, serve.start_year AS start_year, serve.end_year AS end_year
      | GROUP BY $-.start_year YIELD COUNT($-.id), $-.start_year AS start_year, AVG($-.end_year) as avg
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT($-.id) | start_year | avg    |
      | 1            | 2016       | 2017.0 |
      | 1            | 2013       | 2015.0 |
      | 1            | 2009       | 2010.0 |
      | 1            | 2012       | 2013.0 |
      | 1            | 2017       | 2018.0 |
      | 1            | 2007       | 2009.0 |
      | 2            | 2018       | 2018.5 |
      | 1            | 2010       | 2012.0 |
      | 1            | 2015       | 2016.0 |
    When executing query:
      """
      GO FROM hash("Aron Baynes"),hash("Tracy McGrady") OVER serve YIELD $$.team.name AS name, serve._dst AS id, serve.start_year AS start, serve.end_year AS end
      | GROUP BY teamName, start_year YIELD $-.name AS teamName, $-.start AS start_year, MAX($-.start),
      MIN($-.end), AVG($-.end) AS avg_end_year, STD($-.end) AS std_end_year, COUNT($-.id)
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM hash("Carmelo Anthony"),hash("Dwyane Wade") OVER like YIELD $$.player.name AS name | GROUP BY $-.name
      YIELD $-.name AS name, SUM(1.5) AS sum, COUNT(*) AS count, 1+1 AS cal
      """
    Then the result should be, in any order, with relax comparison:
      | name              | sum | count | cal |
      | "Dwyane Wade"     | 1.5 | 1     | 2   |
      | "Chris Paul"      | 3.0 | 2     | 2   |
      | "Carmelo Anthony" | 1.5 | 1     | 2   |
      | "LeBron James"    | 3.0 | 2     | 2   |
    When executing query:
      """
      GO FROM hash("Paul Gasol") OVER like YIELD $$.player.age AS age, like._dst AS id
      | GROUP BY $-.id YIELD $-.id AS id, SUM($-.age) AS age
      | GO FROM $-.id OVER serve YIELD $$.team.name AS name, $-.age AS sumAge
      """
    Then the result should be, in any order, with relax comparison:
      | name        | sumAge |
      | "Grizzlies" | 34     |
      | "Raptors"   | 34     |
      | "Lakers"    | 40     |

  Scenario: Groupby with all agg functions
    When executing query:
      """
      GO FROM hash("Carmelo Anthony"), hash("Dwyane Wade") OVER like
      YIELD $$.player.name AS name, $$.player.age AS dst_age, $$.player.age AS src_age, like.likeness AS likeness
      | GROUP BY $-.name YIELD $-.name AS name, SUM($-.dst_age) AS sum_dst_age, AVG($-.dst_age) AS avg_dst_age,
      MAX($-.src_age) AS max_src_age, MIN($-.src_age) AS min_src_age, BIT_AND(1) AS bit_and, BIT_OR(2) AS bit_or, BIT_XOR(3) AS bit_xor,
      COUNT($-.likeness)
      """
    Then the result should be, in any order, with relax comparison:
      | name              | sum_dst_age | avg_dst_age | max_src_age | min_src_age | bit_and | bit_or | bit_xor | COUNT($-.likeness) |
      | "Carmelo Anthony" | 34          | 34.0        | 34          | 34          | 1       | 2      | 3       | 1                  |
      | "Dwyane Wade"     | 37          | 37.0        | 37          | 37          | 1       | 2      | 3       | 1                  |
      | "Chris Paul"      | 66          | 33.0        | 33          | 33          | 1       | 2      | 0       | 2                  |
      | "LeBron James"    | 68          | 34.0        | 34          | 34          | 1       | 2      | 0       | 2                  |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst as dst
      | GO FROM $-.dst over like YIELD $-.dst as dst, like._dst == hash('Tim Duncan') as following
      | GROUP BY $-.dst YIELD $-.dst AS dst, BIT_OR($-.following) AS following
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | dst             | following |
      | "Manu Ginobili" | BAD_TYPE  |
      | "Tony Parker"   | BAD_TYPE  |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst as dst
      | GO FROM $-.dst over like YIELD $-.dst as dst, like._dst == hash('Tim Duncan') as following
      | GROUP BY $-.dst YIELD $-.dst AS dst, BIT_OR(case when $-.following==true then 1 else 0 end) AS following
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | dst             | following |
      | "Tony Parker"   | 1         |
      | "Manu Ginobili" | 1         |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst as dst
      |  GO FROM $-.dst over like YIELD $-.dst as dst, like._dst == hash('Tim Duncan') as following
      | GROUP BY $-.dst YIELD $-.dst AS dst, BIT_AND($-.following) AS following
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | dst             | following |
      | "Manu Ginobili" | BAD_TYPE  |
      | "Tony Parker"   | BAD_TYPE  |
    When executing query:
      """
      GO FROM hash('Tim Duncan') OVER like YIELD like._dst as dst
      | GO FROM $-.dst over like YIELD $-.dst as dst, like._dst == hash('Tim Duncan') as following
      | GROUP BY $-.dst YIELD $-.dst AS dst, BIT_AND(case when $-.following==true then 1 else 0 end) AS following
      """
    Then the result should be, in any order, with relax comparison, and the columns 0 should be hashed:
      | dst             | following |
      | "Manu Ginobili" | 1         |
      | "Tony Parker"   | 0         |

  Scenario: Groupby with COUNT_DISTINCT
    When executing query:
      """
      GO FROM
        hash('Carmelo Anthony'),
        hash('Dwyane Wade')
      OVER like
      YIELD
        $$.player.name AS name,
        $$.player.age AS dst_age,
        $$.player.age AS src_age,
        like.likeness AS likeness |
      GROUP BY $-.name
      YIELD
        $-.name AS name,
        SUM($-.dst_age) AS sum_dst_age,
        AVG($-.dst_age) AS avg_dst_age,
        MAX($-.src_age) AS max_src_age,
        MIN($-.src_age) AS min_src_age,
        BIT_AND(1) AS bit_and,
        BIT_OR(2) AS bit_or,
        BIT_XOR(3) AS bit_xor,
        COUNT($-.likeness),
        COUNT(DISTINCT $-.likeness)
      """
    Then the result should be, in any order:
      | name              | sum_dst_age | avg_dst_age | max_src_age | min_src_age | bit_and | bit_or | bit_xor | COUNT($-.likeness) | COUNT(distinct $-.likeness) |
      | "Carmelo Anthony" | 34          | 34.0        | 34          | 34          | 1       | 2      | 3       | 1                  | 1                           |
      | "Dwyane Wade"     | 37          | 37.0        | 37          | 37          | 1       | 2      | 3       | 1                  | 1                           |
      | "Chris Paul"      | 66          | 33.0        | 33          | 33          | 1       | 2      | 0       | 2                  | 1                           |
      | "LeBron James"    | 68          | 34.0        | 34          | 34          | 1       | 2      | 0       | 2                  | 1                           |

  Scenario: Groupby works with orderby or limit test
    When executing query:
      """
      GO FROM hash("Carmelo Anthony"),hash("Dwyane Wade") OVER like YIELD $$.player.name AS name | GROUP BY $-.name
      YIELD $-.name AS name, SUM(1.5) AS sum, COUNT(*) AS count | ORDER BY $-.sum, $-.name
      """
    Then the result should be, in order, with relax comparison:
      | name              | sum | count |
      | "Carmelo Anthony" | 1.5 | 1     |
      | "Dwyane Wade"     | 1.5 | 1     |
      | "Chris Paul"      | 3.0 | 2     |
      | "LeBron James"    | 3.0 | 2     |
    When executing query:
      """
      GO FROM hash("Carmelo Anthony"),hash("Dwyane Wade") OVER like YIELD $$.player.name AS name | GROUP BY $-.name
      YIELD $-.name AS name, SUM(1.5) AS sum, COUNT(*) AS count | ORDER BY $-.sum, $-.name DESC | LIMIT 2
      """
    Then the result should be, in order, with relax comparison:
      | name              | sum | count |
      | "Dwyane Wade"     | 1.5 | 1     |
      | "Carmelo Anthony" | 1.5 | 1     |

  Scenario: Empty input
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as name
      | GROUP BY $-.name YIELD $-.name AS name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER like YIELD $$.player.name AS name | GROUP BY $-.name
      YIELD $-.name AS name, SUM(1.5) AS sum, COUNT(*) AS count | ORDER BY $-.sum | LIMIT 2
      """
    Then the result should be, in order, with relax comparison:
      | name | sum | count |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team
      | YIELD $-.name as name WHERE $-.start > 20000 | GROUP BY $-.name YIELD $-.name AS name
      """
    Then the result should be, in any order, with relax comparison:
      | name |
    When executing query:
      """
      GO FROM hash("NON EXIST VERTEX ID") OVER serve YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team
      | YIELD $-.name as name WHERE $-.start > 20000 | Limit 1
      """
    Then the result should be, in any order, with relax comparison:
      | name |

  Scenario: Duplicate column
    When executing query:
      """
      GO FROM hash("Marco Belinelli") OVER serve YIELD $$.team.name AS name, serve._dst AS id, serve.start_year AS start_year, serve.end_year AS start_year
      | GROUP BY $-.start_year YIELD COUNT($-.id), $-.start_year AS start_year, AVG($-.end_year) as avg
      """
    Then a SemanticError should be raised at runtime:

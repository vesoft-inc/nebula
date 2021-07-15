Feature: Basic Aggregate and GroupBy

  Background:
    Given a graph with space named "nba"

  Scenario: Basic Aggregate
    When executing query:
      """
      YIELD COUNT(*), 1+1
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(*) | (1+1) |
      | 1        | 2     |
    When executing query:
      """
      YIELD COUNT(*)+1 ,1+2 ,(INT)abs(count(2))
      """
    Then the result should be, in any order, with relax comparison:
      | (COUNT(*)+1) | (1+2) | (INT)abs(count(2)) |
      | 2            | 3     | 1                  |
    When executing query:
      """
      YIELD count(null) AS v1,
            avg(null) AS v2,
            sum(null) AS v3,
            std(null) AS v4,
            min(null) AS v5,
            max(null) AS v6,
            bit_and(null) AS v9,
            bit_or(null) AS v10,
            bit_xor(null) AS v11
      """
    Then the result should be, in any order, with relax comparison:
      | v1 | v2   | v3 | v4   | v5   | v6   | v9   | v10  | v11  |
      | 0  | NULL | 0  | NULL | NULL | NULL | NULL | NULL | NULL |
    When executing query:
      """
      YIELD 3 AS x | YIELD case COUNT($-.x) == 1 when true THEN 1 ELSE 0 END as a
      """
    Then the result should be, in any order, with relax comparison:
      | a |
      | 1 |

  Scenario: [1] Basic GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 2     |

  Scenario: [2] Basic GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD DISTINCT count(*) AS count where $-.age > 40
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 1     |

  Scenario: [3] Basic GroupBy
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      YIELD count($var.dst) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 2     |

  Scenario: [4] Basic GroupBy
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD DISTINCT like._dst AS dst, $$.player.age AS age;
      YIELD count($var.dst) AS count where $var.age > 40
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 1     |

  Scenario: [5] Basic GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 36.0 |
      | "Manu Ginobili" | 41.0 |

  Scenario: [6] Basic GroupBy
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 36.0 |
      | "Manu Ginobili" | 41.0 |

  Scenario: [7] Basic GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 36.0 |
      | "Manu Ginobili" | 41.0 |

  Scenario: [8] Basic GroupBy
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      YIELD DISTINCT $var.dst AS dst, avg(distinct $var.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 36.0 |
      | "Manu Ginobili" | 41.0 |

  Scenario: [9] Basic GroupBy
    When executing query:
      """
      GO 2 STEPS FROM "Tim Duncan" OVER like YIELD like._dst as dst,like._src AS src, $$.player.age AS age
      | GROUP BY $-.dst YIELD $-.dst AS dst,collect_set($-.src) AS src,collect($-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 | src                              | age      |
      | "Manu Ginobili"     | {"Tony Parker"}                  | [41]     |
      | "Tim Duncan"        | {"Tony Parker", "Manu Ginobili"} | [42, 42] |
      | "LaMarcus Aldridge" | {"Tony Parker"}                  | [33]     |

  Scenario: [10] Basic GroupBy
    When executing query:
      """
      $var=GO 2 STEPS FROM "Tim Duncan" OVER like YIELD like._dst as dst,like._src AS src, $$.player.age AS age;
      YIELD $var.dst AS dst,collect_set($var.src) AS src,collect($var.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 | src                              | age      |
      | "Manu Ginobili"     | {"Tony Parker"}                  | [41]     |
      | "Tim Duncan"        | {"Tony Parker", "Manu Ginobili"} | [42, 42] |
      | "LaMarcus Aldridge" | {"Tony Parker"}                  | [33]     |

  Scenario: [11] Basic GroupBy
    When executing query:
      """
      GO FROM 'Aron Baynes', 'Tracy McGrady' OVER serve
         YIELD $$.team.name AS name,
               serve._dst AS id,
               serve.start_year AS start_year,
               serve.end_year AS end_year
         | GROUP BY $-.name, $-.start_year
           YIELD $-.name AS teamName,
                 $-.start_year AS start_year,
                 MAX($-.start_year),
                 MIN($-.end_year),
                 AVG($-.end_year) AS avg_end_year,
                 STD($-.end_year) AS std_end_year,
                 COUNT($-.id)
      """
    Then the result should be, in any order, with relax comparison:
      | teamName  | start_year | MAX($-.start_year) | MIN($-.end_year) | avg_end_year | std_end_year | COUNT($-.id) |
      | "Celtics" | 2017       | 2017               | 2019             | 2019.0       | 0.0          | 1            |
      | "Magic"   | 2000       | 2000               | 2004             | 2004.0       | 0.0          | 1            |
      | "Pistons" | 2015       | 2015               | 2017             | 2017.0       | 0.0          | 1            |
      | "Raptors" | 1997       | 1997               | 2000             | 2000.0       | 0.0          | 1            |
      | "Rockets" | 2004       | 2004               | 2010             | 2010.0       | 0.0          | 1            |
      | "Spurs"   | 2013       | 2013               | 2013             | 2014.0       | 1.0          | 2            |

  Scenario: [12] Basic GroupBy
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
         serve._dst AS id,
         serve.start_year AS start_year,
         serve.end_year AS end_year
      | GROUP BY $-.start_year
        YIELD COUNT($-.id),
              $-.start_year AS start_year,
              AVG($-.end_year) as avg
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT($-.id) | start_year | avg    |
      | 2            | 2018       | 2018.5 |
      | 1            | 2017       | 2018.0 |
      | 1            | 2016       | 2017.0 |
      | 1            | 2009       | 2010.0 |
      | 1            | 2007       | 2009.0 |
      | 1            | 2012       | 2013.0 |
      | 1            | 2013       | 2015.0 |
      | 1            | 2015       | 2016.0 |
      | 1            | 2010       | 2012.0 |

  Scenario: [13] Basic GroupBy
    When executing query:
      """
      GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
         YIELD $$.player.name AS name,
               $$.player.age AS dst_age,
               $$.player.age AS src_age,
               like.likeness AS likeness
         | GROUP BY $-.name
           YIELD $-.name AS name,
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
    Then the result should be, in any order, with relax comparison:
      | name              | sum_dst_age | avg_dst_age | max_src_age | min_src_age | bit_and | bit_or | bit_xor | COUNT($-.likeness) | COUNT(distinct $-.likeness) |
      | "LeBron James"    | 68          | 34.0        | 34          | 34          | 1       | 2      | 0       | 2                  | 1                           |
      | "Chris Paul"      | 66          | 33.0        | 33          | 33          | 1       | 2      | 0       | 2                  | 1                           |
      | "Dwyane Wade"     | 37          | 37.0        | 37          | 37          | 1       | 2      | 3       | 1                  | 1                           |
      | "Carmelo Anthony" | 34          | 34.0        | 34          | 34          | 1       | 2      | 3       | 1                  | 1                           |

  Scenario: [14] Basic GroupBy
    When executing query:
      """
      GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
         YIELD $$.player.name AS name,
               $$.player.age AS dst_age,
              $$.player.age AS src_age,
              like.likeness AS likeness
         | GROUP BY $-.name
           YIELD $-.name AS name,
                 SUM($-.dst_age) AS sum_dst_age,
                 AVG($-.dst_age) AS avg_dst_age,
                 MAX($-.src_age) AS max_src_age,
                 MIN($-.src_age) AS min_src_age,
                 BIT_AND(1) AS bit_and,
                 BIT_OR(2) AS bit_or,
                 BIT_XOR(3) AS bit_xor,
                 COUNT($-.likeness)
      """
    Then the result should be, in any order, with relax comparison:
      | name              | sum_dst_age | avg_dst_age | max_src_age | min_src_age | bit_and | bit_or | bit_xor | COUNT($-.likeness) |
      | "LeBron James"    | 68          | 34.0        | 34          | 34          | 1       | 2      | 0       | 2                  |
      | "Chris Paul"      | 66          | 33.0        | 33          | 33          | 1       | 2      | 0       | 2                  |
      | "Dwyane Wade"     | 37          | 37.0        | 37          | 37          | 1       | 2      | 3       | 1                  |
      | "Carmelo Anthony" | 34          | 34.0        | 34          | 34          | 1       | 2      | 3       | 1                  |

  Scenario: [15] Basic GroupBy
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst as dst
         | GO FROM $-.dst over like YIELD $-.dst as dst, like._dst == 'Tim Duncan' as following
         | GROUP BY $-.dst
           YIELD $-.dst AS dst, BIT_OR($-.following) AS following
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | following |
      | "Tony Parker"   | BAD_TYPE  |
      | "Manu Ginobili" | BAD_TYPE  |

  Scenario: [16] Basic GroupBy
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst as dst
         | GO FROM $-.dst over like YIELD $-.dst as dst, like._dst == 'Tim Duncan' as following
         | GROUP BY $-.dst
           YIELD $-.dst AS dst,
                 BIT_OR(case when $-.following==true then 1 else 0 end) AS following
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | following |
      | "Tony Parker"   | 1         |
      | "Manu Ginobili" | 1         |

  Scenario: [17] Basic GroupBy
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst as dst
         | GO FROM $-.dst over like YIELD $-.dst as dst, like._dst == 'Tim Duncan' as following
         | GROUP BY $-.dst
           YIELD $-.dst AS dst,
                 BIT_AND($-.following) AS following
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | following |
      | "Tony Parker"   | BAD_TYPE  |
      | "Manu Ginobili" | BAD_TYPE  |

  Scenario: [18] Basic GroupBy
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst as dst
         | GO FROM $-.dst over like YIELD $-.dst as dst, like._dst == 'Tim Duncan' as following
         | GROUP BY $-.dst
           YIELD $-.dst AS dst,
                 BIT_AND(case when $-.following==true then 1 else 0 end) AS following
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | following |
      | "Tony Parker"   | 0         |
      | "Manu Ginobili" | 1         |

  Scenario: [19] Basic GroupBy
    When executing query:
      """
      GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
         YIELD $$.player.name AS name
         | GROUP BY $-.name
           YIELD $-.name AS name,
                 SUM(1.5) AS sum,
                 COUNT(*) AS count,
                 1+1 AS cal
      """
    Then the result should be, in any order, with relax comparison:
      | name              | sum | count | cal |
      | "LeBron James"    | 3.0 | 2     | 2   |
      | "Chris Paul"      | 3.0 | 2     | 2   |
      | "Dwyane Wade"     | 1.5 | 1     | 2   |
      | "Carmelo Anthony" | 1.5 | 1     | 2   |

  Scenario: [20] Basic GroupBy
    When executing query:
      """
      GO FROM 'Paul Gasol' OVER like
         YIELD $$.player.age AS age,
               like._dst AS id
         | GROUP BY $-.id
           YIELD $-.id AS id,
                 SUM($-.age) AS age
           | GO FROM $-.id OVER serve
             YIELD $$.team.name AS name,
                   $-.age AS sumAge
      """
    Then the result should be, in any order, with relax comparison:
      | name        | sumAge |
      | "Grizzlies" | 34     |
      | "Raptors"   | 34     |
      | "Lakers"    | 40     |

  Scenario: GroupBy user defined var
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      GROUP BY $var.age YIELD $var.age AS age, count($var.dst) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | age | count |
      | 36  | 1     |
      | 41  | 1     |

  Scenario: Implicit GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD $-.dst AS dst, 1+avg(distinct $-.age) AS age, abs(5) as abs
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  | abs |
      | "Tony Parker"   | 37.0 | 5   |
      | "Manu Ginobili" | 42.0 | 5   |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD $-.dst AS dst, 1+avg(distinct $-.age) AS age where $-.age > 40
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Manu Ginobili" | 42.0 |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD (INT)($-.age+1) AS age, 1+count(distinct $-.dst) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | age | count |
      | 37  | 2     |
      | 42  | 2     |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      YIELD $var.dst AS dst, (INT)abs(1+avg(distinct $var.age)) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age |
      | "Tony Parker"   | 37  |
      | "Manu Ginobili" | 42  |
    When executing query:
      """
      $var1=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      $var2=GO FROM "Tim Duncan" OVER serve YIELD serve._dst AS dst;
      YIELD $var1.dst AS dst, count($var1.dst) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | count |
      | "Tony Parker"   | 1     |
      | "Manu Ginobili" | 1     |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst
      YIELD
        $-.dst AS dst,
        (INT)(sum($-.age)/count($-.age))+avg(distinct $-.age+1)+1 AS statistics
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | statistics |
      | "Tony Parker"   | 74.0       |
      | "Manu Ginobili" | 84.0       |

  Scenario: Match Implicit GroupBy
    When executing query:
      """
      MATCH (v:player)-[:like]-(m:player)
      WITH m.age AS age, count(m.age) AS count
      RETURN age, count
      ORDER BY age, count
      """
    Then the result should be, in order, with relax comparison:
      | age | count |
      | 20  | 4     |
      | 22  | 2     |
      | 23  | 2     |
      | 25  | 3     |
      | 26  | 1     |
      | 28  | 3     |
      | 29  | 16    |
      | 30  | 7     |
      | 31  | 8     |
      | 32  | 8     |
      | 33  | 14    |
      | 34  | 16    |
      | 36  | 12    |
      | 37  | 6     |
      | 38  | 5     |
      | 39  | 6     |
      | 40  | 8     |
      | 41  | 5     |
      | 42  | 15    |
      | 43  | 3     |
      | 45  | 13    |
      | 46  | 2     |
      | 47  | 3     |
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-[:like]->(m)
      RETURN
        m.name as dst,
        (INT)(sum(m.age)/count(m.age))+avg(distinct m.age+1)+1 AS statistics
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | statistics |
      | "Tony Parker"   | 74.0       |
      | "Manu Ginobili" | 84.0       |
    When executing query:
      """
      MATCH (v:player {name:"noexist"})-[:like]-(m:player)
      WITH m.age AS age, count(m.age) AS count
      RETURN age, count
      ORDER BY age, count
      """
    Then the result should be, in order, with relax comparison:
      | age | count |
    When executing query:
      """
      MATCH (v:player {name:"noexist"})-[:like]-(m:player)
      RETURN count(m.age) AS count,
             sum(m.age) AS sum,
             avg(m.age) AS avg,
             min(m.age) AS min,
             max(m.age) AS max,
             std(m.age) AS std,
             bit_and(m.age) AS b1,
             bit_or(m.age) AS b2,
             bit_xor(m.age) AS b3
      """
    Then the result should be, in order, with relax comparison:
      | count | sum | avg  | min  | max  | std  | b1   | b2   | b3   |
      | 0     | 0   | NULL | NULL | NULL | NULL | NULL | NULL | NULL |
    When executing query:
      """
      UNWIND [1,2,3] AS d RETURN d | YIELD 1 IN COLLECT($-.d) AS b
      """
    Then the result should be, in order, with relax comparison:
      | b    |
      | True |
    When executing query:
      """
      UNWIND [1,2,3] AS d RETURN d | YIELD ANY(l IN COLLECT($-.d) WHERE l==1) AS b
      """
    Then the result should be, in order, with relax comparison:
      | b    |
      | True |

  Scenario: Empty input
    When executing query:
      """
      GO FROM 'noexist' OVER like
         YIELD $$.player.name AS name
         | GROUP BY $-.name
           YIELD $-.name AS name,
                 SUM(1.5) AS sum,
                 COUNT(*) AS count
                | ORDER BY $-.sum
                | LIMIT 2
      """
    Then the result should be, in order, with relax comparison:
      | name | sum | count |
    When executing query:
      """
      GO FROM 'noexist' OVER serve
          YIELD $^.player.name as name,
                serve.start_year as start,
                $$.team.name as team
          | YIELD $-.name as name
            WHERE $-.start > 20000
            | GROUP BY $-.name
              YIELD $-.name AS name
      """
    Then the result should be, in order, with relax comparison:
      | name |
    When executing query:
      """
      GO FROM 'noexist' OVER serve
          YIELD $^.player.name as name,
                serve.start_year as start,
                $$.team.name as team
          | YIELD $-.name as name
            WHERE $-.start > 20000
            | Limit 1
      """
    Then the result should be, in any order, with relax comparison:
      | name |

  Scenario: Duplicate column
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
               serve._dst AS id,
               serve.start_year AS start_year,
               serve.end_year AS start_year
          | GROUP BY $-.start_year
            YIELD COUNT($-.id),
                  $-.start_year AS start_year,
                  AVG($-.end_year) as avg
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM 'noexist' OVER serve
         YIELD $^.player.name as name,
               serve.start_year as start,
              $$.team.name as name
         | GROUP BY $-.name
           YIELD $-.name AS name
      """
    Then a SemanticError should be raised at runtime:

  Scenario: order by and limit
    When executing query:
      """
      GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
         YIELD $$.player.name AS name
         | GROUP BY $-.name
           YIELD $-.name AS name,
                 SUM(1.5) AS sum,
                 COUNT(*) AS count
            | ORDER BY $-.sum, $-.name
      """
    Then the result should be, in any order, with relax comparison:
      | name              | sum | count |
      | "Carmelo Anthony" | 1.5 | 1     |
      | "Dwyane Wade"     | 1.5 | 1     |
      | "Chris Paul"      | 3.0 | 2     |
      | "LeBron James"    | 3.0 | 2     |
    When executing query:
      """
      GO FROM 'Carmelo Anthony', 'Dwyane Wade' OVER like
         YIELD $$.player.name AS name
         | GROUP BY $-.name
           YIELD $-.name AS name,
                 SUM(1.5) AS sum,
                 COUNT(*) AS count
            | ORDER BY $-.sum, $-.name  DESC
            | LIMIT 2
      """
    Then the result should be, in any order, with relax comparison:
      | name              | sum | count |
      | "Carmelo Anthony" | 1.5 | 1     |
      | "Dwyane Wade"     | 1.5 | 1     |

  Scenario: Distinct sum
    When executing query:
      """
      UNWIND [1,2,3,3] AS d RETURN d | YIELD sum(distinct $-.d) AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 6   |

  Scenario: Error Check
    When executing query:
      """
      YIELD avg(*)+1 ,1+2 ,(INT)abs(min(2))
      """
    Then a SyntaxError should be raised at runtime: Could not apply aggregation function on `*` near `avg'
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst,$-.x YIELD avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime:  `$-.x', not exist prop `x'
    When executing query:
      """
      YIELD case COUNT($-.x) == 1 when true THEN 1 ELSE 0 END as a
      """
    Then a SemanticError should be raised at runtime:  `$-.x', not exist prop `x'
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD $-.age+1,age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Not supported expression `age' for props deduction.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age YIELD $-.age,avg(distinct $-.age),$-.dst AS dst
      """
    Then a SemanticError should be raised at runtime: Yield non-agg expression `$-.dst' must be functionally dependent on items in GROUP BY clause
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD $-.age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Yield non-agg expression `$-.age' must be functionally dependent on items in GROUP BY clause
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD $-.age+1,abs(avg(distinct count($-.age))) AS age
      """
    Then a SemanticError should be raised at runtime: Aggregate function nesting is not allowed: `avg(distinct count($-.age))'
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD $-.age+1,avg(distinct count($-.age+1)) AS age
      """
    Then a SemanticError should be raised at runtime: Aggregate function nesting is not allowed: `avg(distinct count(($-.age+1)))'
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY avg($-.age+1)+1 YIELD $-.age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime:  Group `(avg(($-.age+1))+1)' invalid
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD $var.dst AS dst, avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Not support both input and variable in GroupBy sentence.
    When executing query:
      """
      $var1=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      $var2=GO FROM "Tim Duncan" OVER serve YIELD serve._dst AS dst;
      YIELD count($var1.dst),$var2.dst AS count
      """
    Then a SemanticError should be raised at runtime: Only one variable allowed to use.
    When executing query:
      """
      $var1=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      $var2=GO FROM "Tim Duncan" OVER serve YIELD serve._dst AS dst;
      GROUP BY $var1.dst,$var2.dst YIELD 3
      """
    Then a SemanticError should be raised at runtime: Only one variable allowed to use.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD count(*)
      """
    Then a SemanticError should be raised at runtime: `count(*)', not support aggregate function in go sentence.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like where COUNT(*) > 2
      """
    Then a SemanticError should be raised at runtime: `(COUNT(*)>2)', not support aggregate function in where sentence.
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
               serve.end_year AS end_year
         | GROUP BY $-.end_year
           YIELD COUNT($$.team.name)
      """
    Then a SemanticError should be raised at runtime:  Only support input and variable in GroupBy sentence.
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
               serve._dst AS id
         | GROUP BY $-.start_year
           YIELD COUNT($-.id)
      """
    Then a SemanticError should be raised at runtime: `$-.start_year', not exist prop `start_year'
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
        YIELD $$.team.name AS name,
              serve._dst AS id
        | GROUP BY team
          YIELD COUNT($-.id),
                $-.name AS teamName
      """
    Then a SemanticError should be raised at runtime:  Group `team' invalid
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
               serve._dst AS id
         | GROUP BY $-.name
           YIELD COUNT($-.start_year)
      """
    Then a SemanticError should be raised at runtime: `$-.start_year', not exist prop `start_year'
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
               serve._dst AS id
         | GROUP BY $-.name
           YIELD SUM(*)
      """
    Then a SyntaxError should be raised at runtime: Could not apply aggregation function on `*` near `SUM'
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
               serve._dst AS id
         | GROUP BY $-.name
           YIELD COUNT($-.name, $-.id)
      """
    Then a SyntaxError should be raised at runtime: Unknown function  near `COUNT'
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
               serve._dst AS id
         | GROUP BY $-.name, SUM($-.id)
           YIELD $-.name,  SUM($-.id)
      """
    Then a SemanticError should be raised at runtime:  Group `SUM($-.id)' invalid
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
         YIELD $$.team.name AS name,
               COUNT(serve._dst) AS id
      """
    Then a SemanticError should be raised at runtime: `COUNT(serve._dst) AS id', not support aggregate function in go sentence.
    When executing query:
      """
      MATCH (v:player)
      WHERE avg(v.age) > 1
      RETURN v.age
      """
    Then a SyntaxError should be raised at runtime: Invalid use of aggregating function in this context. near `WHERE avg(v.age) > 1'
    When executing query:
      """
      MATCH (v:player)
      WITH v
      WHERE avg(v.age) > 1
      RETURN v.age
      """
    Then a SyntaxError should be raised at runtime: Invalid use of aggregating function in this context. near `WHERE avg(v.age) > 1'
    When executing query:
      """
      MATCH (v:player)
      WITH DISTINCT v
      WHERE avg(v.age) > 1
      RETURN v.age
      """
    Then a SyntaxError should be raised at runtime: Invalid use of aggregating function in this context. near `WHERE avg(v.age) > 1'

# When executing query:
# """
# GO FROM "Marco Belinelli" OVER serve
# YIELD $$.team.name AS name,
# serve.end_year AS end_year
# | GROUP BY $-.end_year
# YIELD COUNT($var)
# """
# Then a SemanticError should be raised at runtime:

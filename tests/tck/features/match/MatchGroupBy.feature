# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Match GroupBy

  Background:
    Given a graph with space named "nba"

  Scenario: [1] Match GroupBy
    When executing query:
      """
      MATCH(n:player)
        RETURN id(n) AS id,
               count(n) AS count,
               sum(floor(n.age)) AS sum,
               max(n.age) AS max,
               min(n.age) AS min,
               avg(distinct n.age) AS age,
               labels(n) AS lb
          ORDER BY id, count, max, min
          SKIP 10 LIMIT 8;
      """
    Then the result should be, in order, with relax comparison:
      | id                      | count | sum  | max | min | age  | lb         |
      | "David West"            | 1     | 38.0 | 38  | 38  | 38.0 | ["player"] |
      | "DeAndre Jordan"        | 1     | 30.0 | 30  | 30  | 30.0 | ["player"] |
      | "Dejounte Murray"       | 1     | 29.0 | 29  | 29  | 29.0 | ["player"] |
      | "Dirk Nowitzki"         | 1     | 40.0 | 40  | 40  | 40.0 | ["player"] |
      | "Dwight Howard"         | 1     | 33.0 | 33  | 33  | 33.0 | ["player"] |
      | "Dwyane Wade"           | 1     | 37.0 | 37  | 37  | 37.0 | ["player"] |
      | "Giannis Antetokounmpo" | 1     | 24.0 | 24  | 24  | 24.0 | ["player"] |
      | "Grant Hill"            | 1     | 46.0 | 46  | 46  | 46.0 | ["player"] |

  Scenario: [2] Match GroupBy
    When executing query:
      """
      MATCH(n:player)
        RETURN id(n) AS id,
               count(n) AS count,
               sum(floor(n.age)) AS sum,
               max(n.age) AS max,
               min(n.age) AS min,
               avg(distinct n.age)+1 AS age,
               labels(n) AS lb
          ORDER BY id, count, max, min
          SKIP 10 LIMIT 8;
      """
    Then the result should be, in order, with relax comparison:
      | id                      | count | sum  | max | min | age  | lb         |
      | "David West"            | 1     | 38.0 | 38  | 38  | 39.0 | ["player"] |
      | "DeAndre Jordan"        | 1     | 30.0 | 30  | 30  | 31.0 | ["player"] |
      | "Dejounte Murray"       | 1     | 29.0 | 29  | 29  | 30.0 | ["player"] |
      | "Dirk Nowitzki"         | 1     | 40.0 | 40  | 40  | 41.0 | ["player"] |
      | "Dwight Howard"         | 1     | 33.0 | 33  | 33  | 34.0 | ["player"] |
      | "Dwyane Wade"           | 1     | 37.0 | 37  | 37  | 38.0 | ["player"] |
      | "Giannis Antetokounmpo" | 1     | 24.0 | 24  | 24  | 25.0 | ["player"] |
      | "Grant Hill"            | 1     | 46.0 | 46  | 46  | 47.0 | ["player"] |

  Scenario: [3] Match GroupBy
    When executing query:
      """
      MATCH(n:player)
        RETURN id(n) AS id,
               count(n) AS count,
               sum(floor(n.age)) AS sum,
               max(n.age) AS max,
               min(n.age) AS min,
               (INT)avg(distinct n.age)+1 AS age,
               labels(n) AS lb
          ORDER BY id, count, max, min
          SKIP 10 LIMIT 8;
      """
    Then the result should be, in order, with relax comparison:
      | id                      | count | sum  | max | min | age | lb         |
      | "David West"            | 1     | 38.0 | 38  | 38  | 39  | ["player"] |
      | "DeAndre Jordan"        | 1     | 30.0 | 30  | 30  | 31  | ["player"] |
      | "Dejounte Murray"       | 1     | 29.0 | 29  | 29  | 30  | ["player"] |
      | "Dirk Nowitzki"         | 1     | 40.0 | 40  | 40  | 41  | ["player"] |
      | "Dwight Howard"         | 1     | 33.0 | 33  | 33  | 34  | ["player"] |
      | "Dwyane Wade"           | 1     | 37.0 | 37  | 37  | 38  | ["player"] |
      | "Giannis Antetokounmpo" | 1     | 24.0 | 24  | 24  | 25  | ["player"] |
      | "Grant Hill"            | 1     | 46.0 | 46  | 46  | 47  | ["player"] |

  Scenario: [4] Match GroupBy
    When executing query:
      """
      MATCH(n:player)
        WHERE n.age > 35
        RETURN DISTINCT id(n) AS id,
                        count(n) AS count,
                        sum(floor(n.age)) AS sum,
                        max(n.age) AS max,
                        min(n.age) AS min,
                        avg(distinct n.age)+1 AS age,
                        labels(n) AS lb
              ORDER BY id, count, max, min
              SKIP 10 LIMIT 6;
      """
    Then the result should be, in order, with relax comparison:
      | id                | count | sum  | max | min | age  | lb                     |
      | "Ray Allen"       | 1     | 43.0 | 43  | 43  | 44.0 | ["player"]             |
      | "Shaquile O'Neal" | 1     | 47.0 | 47  | 47  | 48.0 | ["player"]             |
      | "Steve Nash"      | 1     | 45.0 | 45  | 45  | 46.0 | ["player"]             |
      | "Tim Duncan"      | 1     | 42.0 | 42  | 42  | 43.0 | ["bachelor", "player"] |
      | "Tony Parker"     | 1     | 36.0 | 36  | 36  | 37.0 | ["player"]             |
      | "Tracy McGrady"   | 1     | 39.0 | 39  | 39  | 40.0 | ["player"]             |

  Scenario: [5] Match GroupBy
    When executing query:
      """
      MATCH(n:player)-[:like]->(m)
        WHERE n.age > 35
        RETURN DISTINCT id(n) AS id,
                        count(n) AS count,
                        sum(floor(n.age)) AS sum,
                        max(m.age) AS max,
                        min(n.age) AS min,
                        avg(distinct n.age)+1 AS age,
                        labels(m) AS lb
              ORDER BY id, count, max, min
              SKIP 10 LIMIT 20;
      """
    Then the result should be, in order, with relax comparison:
      | id                | count | sum   | max | min | age  | lb                     |
      | "Shaquile O'Neal" | 1     | 47.0  | 31  | 47  | 48.0 | ["player"]             |
      | "Shaquile O'Neal" | 1     | 47.0  | 42  | 47  | 48.0 | ["bachelor", "player"] |
      | "Steve Nash"      | 4     | 180.0 | 45  | 45  | 46.0 | ["player"]             |
      | "Tim Duncan"      | 2     | 84.0  | 41  | 42  | 43.0 | ["player"]             |
      | "Tony Parker"     | 1     | 36.0  | 42  | 36  | 37.0 | ["bachelor", "player"] |
      | "Tony Parker"     | 2     | 72.0  | 41  | 36  | 37.0 | ["player"]             |
      | "Tracy McGrady"   | 3     | 117.0 | 46  | 39  | 40.0 | ["player"]             |
      | "Vince Carter"    | 2     | 84.0  | 45  | 42  | 43.0 | ["player"]             |
      | "Yao Ming"        | 2     | 76.0  | 47  | 38  | 39.0 | ["player"]             |

  Scenario: [6] Match GroupBy
    When executing query:
      """
      MATCH(n:player)-[:like*2]->(m)-[:serve]->()
        WHERE n.age > 35
        RETURN DISTINCT id(n) AS id,
                        count(n) AS count,
                        sum(floor(n.age)) AS sum,
                        max(m.age) AS max,
                        min(n.age) AS min,
                        avg(distinct n.age)+1 AS age,
                        labels(m) AS lb
              ORDER BY id, count, max, min
        | YIELD count(*) AS count;
      """
    Then the result should be, in order, with relax comparison:
      | count |
      | 20    |

  Scenario: [7] Match GroupBy
    When executing query:
      """
      MATCH(n:player)-[:like*2]->(m)-[:serve]->()
        WHERE n.age > 35
        RETURN DISTINCT id(n) AS id,
                        count(n) AS count,
                        sum(floor(n.age)) AS sum,
                        max(m.age) AS max,
                        min(n.age) AS min,
                        avg(distinct n.age)+1 AS age,
                        labels(m) AS lb
              ORDER BY id, count, max, min
        | YIELD DISTINCT $-.min AS min, (INT)count(*) AS count;
      """
    Then the result should be, in any order, with relax comparison:
      | min | count |
      | 39  | 1     |
      | 42  | 3     |
      | 47  | 1     |
      | 43  | 1     |
      | 38  | 3     |
      | 40  | 1     |
      | 36  | 5     |
      | 37  | 1     |
      | 46  | 1     |
      | 45  | 2     |
      | 41  | 1     |

  Scenario: [8] Match GroupBy
    When executing query:
      """
      MATCH p = (a:player)-[:like]->(other:player)
          WHERE other <> a
        WITH a AS a, other AS other, length(p) AS len
        RETURN a.name AS name, (INT)avg(other.age) AS age, len
          ORDER BY name, age
          SKIP 3 LIMIT 8
      """
    Then the result should be, in order, with relax comparison:
      | name              | age | len |
      | "Blake Griffin"   | 33  | 1   |
      | "Boris Diaw"      | 39  | 1   |
      | "Carmelo Anthony" | 34  | 1   |
      | "Chris Paul"      | 35  | 1   |
      | "Damian Lillard"  | 33  | 1   |
      | "Danny Green"     | 36  | 1   |
      | "Dejounte Murray" | 33  | 1   |
      | "Dirk Nowitzki"   | 42  | 1   |

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Multi Line Multi Query Parts

  Background:
    Given a graph with space named "nba"

  Scenario: Multi Line Multi Path Patterns
    When executing query:
      """
      USE nba;
      MATCH (m)-[]-(n), (n)-[]-(l) WHERE id(m)=="Tim Duncan"
      RETURN m.player.name AS n1, n.player.name AS n2,
      CASE WHEN l.team.name is not null THEN l.team.name
      WHEN l.player.name is not null THEN l.player.name ELSE "null" END AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3           |
      | "Tim Duncan" | "Aron Baynes" | "Celtics"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons"    |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"      |
      | "Tim Duncan" | "Aron Baynes" | "Tim Duncan" |
      | "Tim Duncan" | "Boris Diaw"  | "Hawks"      |
      | "Tim Duncan" | "Boris Diaw"  | "Hornets"    |
      | "Tim Duncan" | "Boris Diaw"  | "Jazz"       |
      | "Tim Duncan" | "Boris Diaw"  | "Spurs"      |
      | "Tim Duncan" | "Boris Diaw"  | "Suns"       |
      | "Tim Duncan" | "Boris Diaw"  | "Tim Duncan" |
    When executing query:
      """
      MATCH (m)-[]-(n), (n)-[]-(l) WHERE id(n)=="Tim Duncan"
      RETURN m.player.name AS n1, n.player.name AS n2, l.player.name AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then the result should be, in order:
      | n1            | n2           | n3                  |
      | "Aron Baynes" | "Tim Duncan" | "Aron Baynes"       |
      | "Aron Baynes" | "Tim Duncan" | "Boris Diaw"        |
      | "Aron Baynes" | "Tim Duncan" | "Danny Green"       |
      | "Aron Baynes" | "Tim Duncan" | "Danny Green"       |
      | "Aron Baynes" | "Tim Duncan" | "Dejounte Murray"   |
      | "Aron Baynes" | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Aron Baynes" | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Aron Baynes" | "Tim Duncan" | "Manu Ginobili"     |
      | "Aron Baynes" | "Tim Duncan" | "Manu Ginobili"     |
      | "Aron Baynes" | "Tim Duncan" | "Manu Ginobili"     |
    When executing query:
      """
      MATCH (m)-[]-(n), (n)-[]-(l), (l)-[]-(h) WHERE id(m)=="Tim Duncan"
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                 |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"        |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquille O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"       |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"       |

  Scenario: Multi Line Multi Match
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      MATCH (n)-[]-(l)
      RETURN m.player.name AS n1, n.player.name AS n2,
      CASE WHEN l.player.name is not null THEN l.player.name
      WHEN l.team.name is not null THEN l.team.name ELSE "null" END AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3           |
      | "Tim Duncan" | "Aron Baynes" | "Celtics"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons"    |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"      |
      | "Tim Duncan" | "Aron Baynes" | "Tim Duncan" |
      | "Tim Duncan" | "Boris Diaw"  | "Hawks"      |
      | "Tim Duncan" | "Boris Diaw"  | "Hornets"    |
      | "Tim Duncan" | "Boris Diaw"  | "Jazz"       |
      | "Tim Duncan" | "Boris Diaw"  | "Spurs"      |
      | "Tim Duncan" | "Boris Diaw"  | "Suns"       |
      | "Tim Duncan" | "Boris Diaw"  | "Tim Duncan" |
    When executing query:
      """
      MATCH (m)-[]-(n),(n) WHERE id(m)=="Tim Duncan" and id(n)=="Tony Parker"
      MATCH (n)-[]-(l) where n.player.age<m.player.age
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 64    |
    When executing query:
      """
      MATCH (v:player) WHERE v.player.age>43
      MATCH (n:player) WHERE v.player.age>40 and n.player.age>v.player.age
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 5     |
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      MATCH (n)-[]-(l), (l)-[]-(h)
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                 |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"        |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquille O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"       |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"       |
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      MATCH (n)-[]-(l)
      MATCH (l)-[]-(h)
      RETURN m.player.name AS n1, n.player.name AS n2, l.team.name AS n3, h.player.name AS n4
      ORDER BY n1, n2, n3, n4 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2            | n3        | n4                 |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Kyrie Irving"     |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Rajon Rondo"      |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Ray Allen"        |
      | "Tim Duncan" | "Aron Baynes" | "Celtics" | "Shaquille O'Neal" |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Blake Griffin"    |
      | "Tim Duncan" | "Aron Baynes" | "Pistons" | "Grant Hill"       |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Aron Baynes"      |
      | "Tim Duncan" | "Aron Baynes" | "Spurs"   | "Boris Diaw"       |
    When executing query:
      """
      MATCH (v:player{name:"Tony Parker"})
      WITH v AS a
      MATCH p=(o:player{name:"Tim Duncan"})-[]->(a)
      RETURN o.player.name
      """
    Then the result should be, in order:
      | o.player.name |
      | "Tim Duncan"  |
      | "Tim Duncan"  |

  Scenario: Multi Line Optional Match
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      OPTIONAL MATCH (n)<-[:serve]-(l)
      RETURN m.player.name AS n1, n.player.name AS n2, l AS n3 ORDER BY n1, n2, n3 LIMIT 10
      """
    Then the result should be, in order:
      | n1           | n2                  | n3   |
      | "Tim Duncan" | "Aron Baynes"       | NULL |
      | "Tim Duncan" | "Boris Diaw"        | NULL |
      | "Tim Duncan" | "Danny Green"       | NULL |
      | "Tim Duncan" | "Danny Green"       | NULL |
      | "Tim Duncan" | "Dejounte Murray"   | NULL |
      | "Tim Duncan" | "LaMarcus Aldridge" | NULL |
      | "Tim Duncan" | "LaMarcus Aldridge" | NULL |
      | "Tim Duncan" | "Manu Ginobili"     | NULL |
      | "Tim Duncan" | "Manu Ginobili"     | NULL |
      | "Tim Duncan" | "Manu Ginobili"     | NULL |
    When executing query:
      """
      MATCH (m)-[]->(n) WHERE id(m)=="Tim Duncan"
      OPTIONAL MATCH (n)-[]->(l) WHERE id(n)=="Tony Parker"
      RETURN id(m) AS m, id(n) AS n, id(l) AS l;
      """
    Then the result should be, in any order:
      | m            | n                   | l                   |
      | "Tim Duncan" | "Spurs"             | NULL                |
      | "Tim Duncan" | "LaMarcus Aldridge" | NULL                |
      | "Tim Duncan" | "Tony Parker"       | "Spurs"             |
      | "Tim Duncan" | "Tony Parker"       | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"       | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"       | "Kyle Anderson"     |
      | "Tim Duncan" | "Tony Parker"       | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"       | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"       | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"       | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"       | "Hornets"           |
      | "Tim Duncan" | "Tony Parker"       | "Spurs"             |
      | "Tim Duncan" | "Tony Parker"       | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"       | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"       | "Kyle Anderson"     |
      | "Tim Duncan" | "Tony Parker"       | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"       | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"       | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"       | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"       | "Hornets"           |
      | "Tim Duncan" | "Danny Green"       | NULL                |
      | "Tim Duncan" | "Manu Ginobili"     | NULL                |
      | "Tim Duncan" | "Manu Ginobili"     | NULL                |
    When executing query:
      """
      OPTIONAL match (v:player) WHERE v.player.age > 41
      MATCH (v:player) WHERE v.player.age>40
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 7     |
    When executing query:
      """
      OPTIONAL match (v:player) WHERE v.player.age>43
      MATCH (n:player) WHERE n.player.age>40
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 32    |
    When executing query:
      """
      OPTIONAL MATCH (v:player) WHERE v.player.age > 40 and v.player.age<46
      MATCH (v:player) WHERE v.player.age>43
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 2     |
    When executing query:
      """
      MATCH (v:player) WHERE v.player.age > 40 and v.player.age<46
      OPTIONAL MATCH (v:player) WHERE v.player.age>43
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 6     |
    When executing query:
      """
      OPTIONAL MATCH (v:player) WHERE v.player.age > 40 and v.player.age<46
      OPTIONAL MATCH (v:player) WHERE v.player.age>43
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 6     |
    When executing query:
      """
      OPTIONAL MATCH (v:player) WHERE v.player.age>43
      MATCH (v:player) WHERE v.player.age > 40 and v.player.age<46
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 2     |
    When executing query:
      """
      MATCH (v:player) WHERE v.player.age>43
      OPTIONAL MATCH (v:player) WHERE v.player.age > 40 and v.player.age<46
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 4     |
    When executing query:
      """
      OPTIONAL MATCH (v:player) WHERE v.player.age>43
      OPTIONAL MATCH (v:player) WHERE v.player.age > 40 and v.player.age<46
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 4     |

  Scenario: Multi Line Multi Query Parts
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      WITH n, n.player.name AS n1 ORDER BY n1 LIMIT 10
      MATCH (n)-[]-(l)
      RETURN n.player.name AS n1,
      CASE WHEN l.player.name is not null THEN l.player.name
      WHEN l.team.name is not null THEN l.team.name ELSE "null" END AS n2 ORDER BY n1, n2 LIMIT 10
      """
    Then the result should be, in order:
      | n1            | n2           |
      | "Aron Baynes" | "Celtics"    |
      | "Aron Baynes" | "Pistons"    |
      | "Aron Baynes" | "Spurs"      |
      | "Aron Baynes" | "Tim Duncan" |
      | "Boris Diaw"  | "Hawks"      |
      | "Boris Diaw"  | "Hornets"    |
      | "Boris Diaw"  | "Jazz"       |
      | "Boris Diaw"  | "Spurs"      |
      | "Boris Diaw"  | "Suns"       |
      | "Boris Diaw"  | "Tim Duncan" |
    When executing query:
      """
      MATCH (m:player{name:"Tim Duncan"})-[:like]-(n)--()
      WITH  m,count(*) AS lcount
      MATCH (m)--(n)
      RETURN count(*) AS scount, lcount
      """
    Then the result should be, in order:
      | scount | lcount |
      | 19     | 110    |
    When executing query:
      """
      MATCH (m:player{name:"Tim Duncan"})-[:like]-(n)--()
      WITH  m,n
      MATCH (m)--(n)
      RETURN count(*) AS scount
      """
    Then the result should be, in order:
      | scount |
      | 270    |
    When executing query:
      """
      MATCH (a:player{age:42}) WITH a
      MATCH (b:player{age:40}) WHERE b.player.age<a.player.age
      UNWIND [1,2,3] as l
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 12    |
    When executing query:
      """
      MATCH (a:player{age:42}) WITH a
      MATCH (b:player{age:40}) WITH a,b WHERE b.player.age<a.player.age
      UNWIND [1,2,3] as l
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 12    |
    When executing query:
      """
      OPTIONAL match (v:player) WHERE v.player.age>43 WITH v
      MATCH (v:player) WHERE v.player.age>40  WITH v
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 4     |
    When executing query:
      """
      OPTIONAL match (v:player) WHERE v.player.age>43 WITH v
      MATCH (n:player) WHERE n.player.age>40  WITH v, n
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 32    |
    When executing query:
      """
      MATCH (a:player{age:42}) WITH a
      MATCH (b:player{age:40}) WHERE b.player.age<a.player.age
      UNWIND [1,2,3] AS l WITH a,b,l WHERE b.player.age+1 < a.player.age
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 12    |

  Scenario: Multi Line Some Erros
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      WITH n, n.player.name AS n1 ORDER BY n1 LIMIT 10
      RETURN m
      """
    Then a SemanticError should be raised at runtime: Alias used but not defined: `m'
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      OPTIONAL MATCH (n)-->(v) WHERE v.player.age < m.player.age
      RETURN n,v
      """
    Then a SemanticError should be raised at runtime: The where clause of optional match statement that reference variables defined by other statements is not supported yet.
    When executing query:
      """
      MATCH (m)-[]-(n) WHERE id(m)=="Tim Duncan"
      OPTIONAL MATCH (n)-->(v) WHERE id(v) < id(m) RETURN count(*) AS count
      """
    Then a SemanticError should be raised at runtime: The where clause of optional match statement that reference variables defined by other statements is not supported yet.

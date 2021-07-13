# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Predicate

  Scenario: yield a predicate
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD all(n IN range(1, 5) WHERE n > 2) AS r
      """
    Then the result should be, in any order:
      | r     |
      | False |
    When executing query:
      """
      YIELD any(n IN [1, 2, 3, 4, 5] WHERE n > 2) AS r
      """
    Then the result should be, in any order:
      | r    |
      | True |
    When executing query:
      """
      YIELD single(n IN range(1, 5) WHERE n == 3) AS r
      """
    Then the result should be, in any order:
      | r    |
      | True |
    When executing query:
      """
      YIELD none(n IN range(1, 3) WHERE n == 0) AS r
      """
    Then the result should be, in any order:
      | r    |
      | True |

  Scenario: use a predicate in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE any(x IN [5,  10] WHERE like.likeness + $$.player.age + x > 100)
      YIELD like._dst AS id, like.likeness AS likeness
      """
    Then the result should be, in any order:
      | id                  | likeness |
      | "LaMarcus Aldridge" | 90       |
      | "Manu Ginobili"     | 95       |
      | "Tim Duncan"        | 95       |

  Scenario: exists with dynamic map in MATCH
    Given an empty graph
    And load "nba" csv data to a new space
    Given having executed:
      """
      CREATE TAG INDEX bachelor_name_index ON bachelor(name(20));
      """
    And wait 6 seconds
    When submit a job:
      """
      REBUILD TAG INDEX bachelor_name_index
      """
    Then wait the job to finish
    When executing query:
      """
      MATCH (n:bachelor) WHERE EXISTS(n['name']) return n AS bachelor
      """
    Then the result should be, in order:
      | bachelor                                                                                                    |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      MATCH(n:player) WHERE EXISTS(n['name'])
      RETURN n.name AS name ORDER BY name LIMIT 10
      """
    Then the result should be, in order:
      | name                |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
      | "Ben Simmons"       |
      | "Blake Griffin"     |
      | "Boris Diaw"        |
      | "Carmelo Anthony"   |
      | "Chris Paul"        |
      | "Cory Joseph"       |
      | "Damian Lillard"    |
      | "Danny Green"       |
    When executing query:
      """
      MATCH(v:player)-[e:like|serve]-(v2) WHERE EXISTS(e['likeness']) RETURN DISTINCT v2
      """
    Then the result should be, in any order:
      | v2                                                                                                          |
      | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})                                                   |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                                                         |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"})                                                       |
      | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})                                           |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Chris Paul" :player{age: 33, name: "Chris Paul"})                                                         |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})                                               |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | ("Paul George" :player{age: 28, name: "Paul George"})                                                       |
      | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})                                                       |
      | ("Kyrie Irving" :player{age: 26, name: "Kyrie Irving"})                                                     |
      | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                                                           |
      | ("James Harden" :player{age: 29, name: "James Harden"})                                                     |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | ("LeBron James" :player{age: 34, name: "LeBron James"})                                                     |
      | ("Rajon Rondo" :player{age: 33, name: "Rajon Rondo"})                                                       |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})                                               |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | ("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"})                                         |
      | ("Klay Thompson" :player{age: 29, name: "Klay Thompson"})                                                   |
      | ("Stephen Curry" :player{age: 31, name: "Stephen Curry"})                                                   |
      | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | ("Kevin Durant" :player{age: 30, name: "Kevin Durant"})                                                     |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})                                           |
      | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"})                                                         |
      | ("Ben Simmons" :player{age: 22, name: "Ben Simmons"})                                                       |
      | ("JaVale McGee" :player{age: 31, name: "JaVale McGee"})                                                     |
      | ("Joel Embiid" :player{age: 25, name: "Joel Embiid"})                                                       |
      | ("Blake Griffin" :player{age: 30, name: "Blake Griffin"})                                                   |
      | ("Damian Lillard" :player{age: 28, name: "Damian Lillard"})                                                 |
    When executing query:
      """
      MATCH(n:player) WHERE EXISTS("abc")
      RETURN n.name AS name ORDER BY name LIMIT 10
      """
    Then a SyntaxError should be raised at runtime: The exists only accept LabelAttribe, Attribute and Subscript
    Then drop the used space

  Scenario: use a exists with null properties
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player) WHERE not exists(v.name) RETURN id(v)
      """
    Then the result should be, in any order:
      | id(v)   |
      | "Null1" |
      | "Null2" |
      | "Null3" |
      | "Null4" |
    When executing query:
      """
      WITH {a : NULL, b : 1, c : false} AS m RETURN exists(m.a)
      """
    Then the result should be, in any order:
      | exists(m.a) |
      | false       |

  Scenario: exists with dynamic map in variable length MATCH
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH(v:player{name:"Tim Duncan"})-[e:like|serve*2]->(v2)
      RETURN DISTINCT e, ALL(e IN e WHERE EXISTS(e['likeness']))
      """
    Then the result should be, in any order:
      | e                                                                                                                                  | all(e IN e WHERE exists(e["likeness"])) |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]]     | false                                   |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]]   | false                                   |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]]                     | true                                    |
      | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]                 | true                                    |
      | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]] | false                                   |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]]              | true                                    |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}], [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]]                  | true                                    |

  Scenario: use a predicate in MATCH
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m)
      RETURN nodes(p)[0].name AS n1, nodes(p)[1].name AS n2,
      all(n IN nodes(p) WHERE n.name NOT STARTS WITH "D") AS b
      """
    Then the result should be, in any order:
      | n1             | n2                | b     |
      | "LeBron James" | "Carmelo Anthony" | True  |
      | "LeBron James" | "Chris Paul"      | True  |
      | "LeBron James" | "Danny Green"     | False |
      | "LeBron James" | "Dejounte Murray" | False |
      | "LeBron James" | "Dwyane Wade"     | False |
      | "LeBron James" | "Kyrie Irving"    | True  |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)
      RETURN single(n IN nodes(p) WHERE n.age > 40) AS b
      """
    Then the result should be, in any order:
      | b    |
      | True |
    When executing query:
      """
      WITH [1, 2, 3, 4, 5, NULL] AS a, 1.3 AS b
      RETURN any(n IN a WHERE n > 2 + b) AS r1
      """
    Then the result should be, in any order:
      | r1   |
      | True |

  Scenario: collection is not a LIST
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD single(n IN "Tom" WHERE n == 3) AS r
      """
    Then a SemanticError should be raised at runtime: `"Tom"', expected LIST, but was STRING
    When executing query:
      """
      YIELD single(n IN NULL WHERE n == 3) AS r
      """
    Then the result should be, in any order:
      | r    |
      | NULL |

  Scenario: aggregate function in collection
    Given a graph with space named "nba"
    When executing query:
      """
      UNWIND [1, 2, 3, 4, 5] AS a RETURN a * 2 AS x | RETURN any(n in collect($-.x) WHERE n > 5) AS myboo
      """
    Then the result should be, in any order:
      | myboo |
      | true  |
    When executing query:
      """
      UNWIND [1, 2, 3, 4, 5] AS a RETURN a * 2 AS x | RETURN All(n in collect($-.x) WHERE n > 5) AS myboo
      """
    Then the result should be, in any order:
      | myboo |
      | false |
    When executing query:
      """
      UNWIND [1, 2, 3, 4, 5] AS a RETURN a * 2 AS x | RETURN single(n in collect($-.x) WHERE n > 5) AS myboo
      """
    Then the result should be, in any order:
      | myboo |
      | false |
    When executing query:
      """
      UNWIND [1, 2, 3, 4, 5] AS a RETURN a * 2 AS x | RETURN None(n in collect($-.x) WHERE n > 5) AS myboo
      """
    Then the result should be, in any order:
      | myboo |
      | false |

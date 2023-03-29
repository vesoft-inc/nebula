Feature: With clause

  Background:
    Given a graph with space named "nba"

  Scenario: with return
    When executing query:
      """
      WITH [1, 2, 3] AS a, "hello" AS b
      RETURN a, b
      """
    Then the result should be, in any order:
      | a       | b       |
      | [1,2,3] | "hello" |
    When executing query:
      """
      WITH [1, 2, 3] AS a WITH a AS a, "hello" AS b
      RETURN a, b
      """
    Then the result should be, in any order, with relax comparison:
      | a       | b       |
      | [1,2,3] | "hello" |
    When executing query:
      """
      WITH [1, 2, 3] AS a
      WITH a, "hello" AS b
      RETURN a, b
      """
    Then the result should be, in any order, with relax comparison:
      | a       | b       |
      | [1,2,3] | "hello" |
    When executing query:
      """
      WITH 1 AS a, 2 AS b
      WITH *
      RETURN *, a + b AS c
      """
    Then the result should be, in any order, with relax comparison:
      | a | b | c |
      | 1 | 2 | 3 |
    When executing query:
      """
      WITH *, "tom" AS a
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | a     |
      | "tom" |

  Scenario: with agg return
    When executing query:
      """
      WITH collect([0, 0.0, 100]) AS n
      RETURN n
      """
    Then the result should be, in any order:
      | n               |
      | [[0, 0.0, 100]] |
    When executing query:
      """
      WITH [1, 2, 3] AS a
      RETURN count(a)
      """
    Then the result should be, in any order:
      | count(a) |
      | 1        |
    When executing query:
      """
      WITH {a:1, b:{c:3, d:{e:5}}} AS x
      RETURN x.b.d.e
      """
    Then the result should be, in any order:
      | x.b.d.e |
      | 5       |
    When executing query:
      """
      WITH {a:1, b:{c:3, d:{e:5}}} AS x
      RETURN x.b.d
      """
    Then the result should be, in any order:
      | x.b.d  |
      | {e: 5} |
    When executing query:
      """
      WITH {a:1, b:{c:3, d:{e:5}}} AS x
      RETURN x.b
      """
    Then the result should be, in any order:
      | x.b               |
      | {c: 3, d: {e: 5}} |
    When executing query:
      """
      WITH {a:1, b:{c:3, d:{e:5}}} AS x
      RETURN x.c
      """
    Then the result should be, in any order:
      | x.c  |
      | NULL |

  Scenario: match with return
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)
      WITH avg(v2.player.age) as average_age
      RETURN average_age
      """
    Then the result should be, in any order, with relax comparison:
      | average_age        |
      | 35.888888888888886 |
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)-[]-(v3)
      WITH v3.player.name as names
      RETURN count(names)
      """
    Then the result should be, in any order, with relax comparison:
      | count(names) |
      | 141          |
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)-[]-(v3)
      WITH v3.team.name as names
      RETURN count(names)
      """
    Then the result should be, in any order, with relax comparison:
      | count(names) |
      | 50           |
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)
      WITH distinct(v2.player.name) AS names
      ORDER by names DESC LIMIT 5
      RETURN collect(names)
      """
    Then the result should be, in any order, with relax comparison:
      | collect(names)                                                           |
      | ["Tony Parker", "Tiago Splitter", "Shaquille O'Neal", "Marco Belinelli"] |
    When profiling query:
      """
      MATCH (v:player)
      WITH v.player.age AS age, v AS v, v.player.name AS name
         ORDER BY age DESCENDING, name ASCENDING
         LIMIT 20
         WHERE age > 30
      RETURN v, age
      """
    Then the result should be, in order, with relax comparison:
      | v                                                                                                           | age |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             | 47  |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         | 46  |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                                                         | 45  |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         | 45  |
      | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                                                           | 43  |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | 42  |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     | 42  |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | 41  |
      | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})                                                   | 40  |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       | 40  |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   | 39  |
      | ("David West" :player{age: 38, name: "David West"})                                                         | 38  |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         | 38  |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             | 38  |
      | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})                                                       | 37  |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})                                           | 36  |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | 36  |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | 36  |
      | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})                                               | 34  |
      | ("LeBron James" :player{age: 34, name: "LeBron James"})                                                     | 34  |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 9  | Project        | 8            |               |
      | 8  | Filter         | 12           |               |
      | 12 | TopN           | 11           |               |
      | 11 | Project        | 3            |               |
      | 3  | AppendVertices | 1            |               |
      | 1  | IndexScan      | 0            |               |
      | 0  | Start          |              |               |
    When executing query:
      """
      MATCH (v:player)-[:like]->(v2)
      WHERE v.player.name == "Tony Parker" and v2.player.age == 42
      WITH *, v.player.age + 100 AS age
      RETURN *, v2.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                     | v2                                                                                                          | age | v2.player.name |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | 136 | "Tim Duncan"   |
    When executing query:
      """
      MATCH (:player)-[:like]->()
      RETURN *
      """
    Then a SemanticError should be raised at runtime: RETURN * is not allowed when there are no variables in scope
    When executing query:
      """
      MATCH (:player)-[:like]->()
      WITH *
      RETURN *
      """
    Then a SemanticError should be raised at runtime: RETURN * is not allowed when there are no variables in scope
    When executing query:
      """
      MATCH (:player {name:"Chris Paul"})-[:serve]->(b)
      WITH collect(b) as teams
      RETURN teams
      """
    Then the result should be, in any order, with relax comparison:
      | teams                                                                                                          |
      | [("Rockets" :team{name: "Rockets"}), ("Clippers" :team{name: "Clippers"}), ("Hornets" :team{name: "Hornets"})] |
    When executing query:
      """
      MATCH (:player {name:"Chris Paul"})-[e:like]->(b)
      WITH avg(e.likeness) as avg, max(e.likeness) as max
      RETURN avg, max
      """
    Then the result should be, in any order, with relax comparison:
      | avg  | max |
      | 90.0 | 90  |
    When executing query:
      """
      MATCH (:player {name:"Tim Duncan"})-[e:like]->(dst)
      WITH dst AS b
      RETURN b.player.age AS age, b
      """
    Then the result should be, in any order, with relax comparison:
      | age | b                                                         |
      | 36  | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
      | 41  | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
    When executing query:
      """
      MATCH (:player {name:"Tim Duncan"})-[e:like]->(dst)
      WITH dst AS b
      RETURN b.age AS age, b
      """
    Then the result should be, in any order, with relax comparison:
      | age  | b                                                         |
      | NULL | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
      | NULL | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |

  @skip
  Scenario: with match return
    When executing query:
      """
      WITH "Tony Parker" AS a
      MATCH (v:player{name: a})
      RETURN v.player.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 23  |

  Scenario: with unwind return
    When executing query:
      """
      WITH [1, 2, 3] AS a, "hello" AS b
      UNWIND a as c
      RETURN c
      """
    Then the result should be, in any order, with relax comparison:
      | c |
      | 1 |
      | 2 |
      | 3 |
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      WITH "hello" AS b, a AS c
      UNWIND c as d
      RETURN b, c, d
      """
    Then the result should be, in any order, with relax comparison:
      | b       | c | d |
      | "hello" | 1 | 1 |
      | "hello" | 2 | 2 |
      | "hello" | 3 | 3 |

  Scenario: with exists
    When executing query:
      """
      WITH {abc:123} AS m, "hello" AS b
      RETURN exists(m.abc), b
      """
    Then the result should be, in any order, with relax comparison:
      | exists(m.abc) | b       |
      | true          | "hello" |
    When executing query:
      """
      WITH {abc:123} AS m, "hello" AS b
      RETURN exists(m.abc), exists(m.a), exists({label:"hello"}.label) as t, exists({hello:123}.a)
      """
    Then the result should be, in any order, with relax comparison:
      | exists(m.abc) | exists(m.a) | t    | exists({hello:123}.a) |
      | true          | false       | true | false                 |
    When executing query:
      """
      WITH [1,2,3] AS m
      RETURN exists(m.abc)
      """
    Then the result should be, in any order, with relax comparison:
      | exists(m.abc) |
      | BAD_TYPE      |
    When executing query:
      """
      WITH null AS m
      RETURN exists(m.abc), exists((null).abc)
      """
    Then the result should be, in any order, with relax comparison:
      | exists(m.abc) | exists(NULL.abc) |
      | NULL          | NULL             |

  Scenario: error check
    When executing query:
      """
      WITH [1, 2, 3] AS a
      WITH a, "hello"
      RETURN a
      """
    Then a SemanticError should be raised at runtime: Expression in WITH must be aliased (use AS)
    When executing query:
      """
      WITH [1, 2, 3] AS a
      WITH a, a+b AS c
      RETURN a
      """
    Then a SemanticError should be raised at runtime:  Alias `b` not defined

  Scenario: condition on nonexist prop
    Given an empty graph
    And having executed:
      """
      CREATE SPACE IF NOT EXISTS `nebula_huskie` (partition_num = 32, replica_factor = 1, charset = utf8, collate = utf8_bin, vid_type = INT64);
      """
    And having executed:
      """
      USE `nebula_huskie`
      """
    And having executed:
      """
      CREATE EDGE IF NOT EXISTS `EDGE_0` (`edgeProp_0_0` bool NULL, `edgeProp_0_1` bool NULL, `edgeProp_0_2` float NULL,
      `edgeProp_0_3` string NULL, `edgeProp_0_4` float NULL)
      ttl_duration = 0, ttl_col = ""
      """
    And having executed:
      """
      CREATE EDGE IF NOT EXISTS `EDGE_1` (`edgeProp_1_0` bool NULL) ttl_duration = 0, ttl_col = ""
      """
    And having executed:
      """
      CREATE TAG IF NOT EXISTS `Label_0` (`labelProp_0_0` float NULL, `labelProp_0_1` string NULL, `labelProp_0_2` bool NULL, `labelProp_0_3` string NULL) ttl_duration = 0, ttl_col = ""
      """
    And wait 3 seconds
    And having executed:
      """
      insert vertex Label_0(labelProp_0_0, labelProp_0_1, labelProp_0_2, labelProp_0_3) values 100:(0.5402889847755432,"Rajon Rondo",true,"Shaquile O'Neal");
      insert vertex Label_0(labelProp_0_0, labelProp_0_1, labelProp_0_2, labelProp_0_3) values 101:(0.5402889847755432,"Rajon Rondo",true,"Shaquile O'Neal");
      insert edge EDGE_0 values 100->101:(true, false, 0.9068049788475037, "Yao Ming", 0.906804);
      insert edge EDGE_1 values 100->101:(true);
      """
    When executing query:
      """
      Match p = (v)-[e]->(t) where id(v) in [100] and e.edgeProp_1_0 == true with e as e, t as t where e.edgeProp_1_0 == true return e;
      """
    Then the result should be, in any order:
      | e                                          |
      | [:EDGE_1 100->101 @0 {edgeProp_1_0: true}] |
    When executing query:
      """
      match p = (v)-[e]->(t) where id(v) in [100] and e.edgeProp_0_0 == true with e as e, t as t where e.edgeProp_0_0 == true return e.edgeProp_0_0;
      """
    Then the result should be, in any order:
      | e.edgeProp_0_0 |
      | true           |
    When executing query:
      """
      match p = (v)-[e]->(t) where id(v) in [100] and e.edgeProp_0_0 == true with e as e, t as t where e.edgeProp_0_0 == true return e.edgeProp_1_0;
      """
    Then the result should be, in any order:
      | e.edgeProp_1_0 |
      | NULL           |

  Scenario: with wildcard after unwind
    When executing query:
      """
      match p = (v0)-[e0]->(v1) where id(v0) in ["Tim Duncan"] unwind v0 as uv0 with * return e0;
      """
    Then the result should be, in any order:
      | e0                                                                                  |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}] |
      | [:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]       |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]                               |
      | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]       |
      | [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]     |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]                             |

  Scenario: with wildcard after multiple matches
    When executing query:
      """
      match (v0:player)--(v1:team) where v1.team.name == "Spurs" and v0.player.name == "Tim Duncan"
      match (v:player) where v.player.name != "Tim Duncan" with v0 where v0.player.age > 0
      match (v0:player)
      with *
      return count(v0)
      """
    Then the result should be, in order:
      | count(v0) |
      | 51        |
    When executing query:
      """
      match (v:player)
      with v AS p
      match (p)
      with p AS v
      match (v)
      with *
      return count (p)
      """
    Then a SemanticError should be raised at runtime:  Alias used but not defined: `p'

  Scenario: with wildcard after unwind before argument
    When executing query:
      """
      match (v:player)--(t:team)
      where id(v) == "Tim Duncan"
      unwind [1] as digit
      with *
      match (t:team)<--(v1)
      return v1.player.name
      """
    Then the result should be, in any order:
      | v1.player.name      |
      | "Cory Joseph"       |
      | "Kyle Anderson"     |
      | "Danny Green"       |
      | "David West"        |
      | "Jonathon Simmons"  |
      | "LaMarcus Aldridge" |
      | "Rudy Gay"          |
      | "Tony Parker"       |
      | "Marco Belinelli"   |
      | "Marco Belinelli"   |
      | "Tiago Splitter"    |
      | "Tim Duncan"        |
      | "Manu Ginobili"     |
      | "Tracy McGrady"     |
      | "Boris Diaw"        |
      | "Aron Baynes"       |
      | "Paul Gasol"        |
      | "Dejounte Murray"   |

  Scenario: invalid input generated by with
    When executing query:
      """
      with "1" as a match (a)-[e:like]->(b) return b
      """
    Then a ExecutionError should be raised at runtime:  Argument only support vertex, but got "1", which is type string
    When executing query:
      """
      with 1 as a match (a)-[e:like]->(b) return b
      """
    Then a ExecutionError should be raised at runtime:  Argument only support vertex, but got 1, which is type int
    When executing query:
      """
      with 1 as b match (b) return b
      """
    Then a ExecutionError should be raised at runtime:  Argument only support vertex, but got 1, which is type int
    When executing query:
      """
      with [1,2] as a unwind a as b  match (b) return b
      """
    Then a ExecutionError should be raised at runtime:  Argument only support vertex, but got 1, which is type int
    When executing query:
      """
      with [1,2] as a unwind a as b  match (b)-[e:like]->(a:player{age:30}) return b
      """
    Then a ExecutionError should be raised at runtime:  Argument only support vertex, but got 1, which is type int

  Scenario: duplicate columns
    When executing query:
      """
      match (v:player{name:"Tim Duncan"})-[e:like]-(v2)
      with v2, sum(e.likeness) as likeness
      match (v2)-[:serve]-(v3)
      return *
      """
    Then the result should be, in any order:
      | v2                                                                | likeness | v3                        |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             | 190      | ("Hornets" :team{})       |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             | 190      | ("Spurs" :team{})         |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             | 80       | ("Celtics" :team{})       |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             | 80       | ("Pistons" :team{})       |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             | 80       | ("Spurs" :team{})         |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         | 185      | ("Spurs" :team{})         |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               | 80       | ("Hawks" :team{})         |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               | 80       | ("Hornets" :team{})       |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               | 80       | ("Jazz" :team{})          |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               | 80       | ("Spurs" :team{})         |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               | 80       | ("Suns" :team{})          |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) | 75       | ("Spurs" :team{})         |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) | 75       | ("Trail Blazers" :team{}) |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             | 70       | ("Cavaliers" :team{})     |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             | 70       | ("Raptors" :team{})       |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             | 70       | ("Spurs" :team{})         |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     | 99       | ("Spurs" :team{})         |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("76ers" :team{})         |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Bulls" :team{})         |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Hawks" :team{})         |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Hornets" :team{})       |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Kings" :team{})         |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Raptors" :team{})       |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Spurs" :team{})         |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Warriors" :team{})      |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Hornets" :team{})       |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | 55       | ("Spurs" :team{})         |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   | 80       | ("Cavaliers" :team{})     |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   | 80       | ("Celtics" :team{})       |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   | 80       | ("Heat" :team{})          |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   | 80       | ("Lakers" :team{})        |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   | 80       | ("Magic" :team{})         |
      | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})   | 80       | ("Suns" :team{})          |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       | 80       | ("76ers" :team{})         |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       | 80       | ("Hawks" :team{})         |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       | 80       | ("Spurs" :team{})         |

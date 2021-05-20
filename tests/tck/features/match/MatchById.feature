# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Match By Id

  Background:
    Given a graph with space named "nba"

  Scenario: single node
    When executing query:
      """
      MATCH (n) WHERE id(n) == 'James Harden' RETURN n
      """
    Then the result should be, in any order, with relax comparison:
      | n                |
      | ("James Harden") |
    When executing query:
      """
      MATCH (n) WHERE id(n) == 'not_exist_vertex' RETURN n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
    When executing query:
      """
      MATCH (n) WHERE id(n) == 'not_exist_vertex' RETURN id(n)
      """
    Then the result should be, in any order, with relax comparison:
      | id(n) |
    When executing query:
      """
      MATCH (n) WHERE id(n) == 'Tony Parker' RETURN id(n), labels(n)
      """
    Then the result should be, in any order, with relax comparison:
      | id(n)         | labels(n)  |
      | 'Tony Parker' | ['player'] |
    When executing query:
      """
      MATCH (n) WHERE id(n) == 'not_exist_vertex' RETURN labels(n)
      """
    Then the result should be, in any order, with relax comparison:
      | labels(n) |

  Scenario: multi node
    When executing query:
      """
      MATCH (n) WHERE id(n) IN ['not_exist_vertex'] return n
      """
    Then the result should be, in any order, with relax comparison:
      | n |
    When executing query:
      """
      MATCH (n) WHERE id(n) IN ['LaMarcus Aldridge', 'Tony Parker'] return n
      """
    Then the result should be, in any order, with relax comparison:
      | n                     |
      | ("LaMarcus Aldridge") |
      | ("Tony Parker")       |
    When executing query:
      """
      MATCH (n) WHERE id(n) IN ['LaMarcus Aldridge', 'Tony Parker', 'not_exist_vertex'] return n
      """
    Then the result should be, in any order, with relax comparison:
      | n                     |
      | ("LaMarcus Aldridge") |
      | ("Tony Parker")       |
    When executing query:
      """
      MATCH (n) WHERE id(n) IN ['LaMarcus Aldridge', 'Tony Parker', 'not_exist_vertex'] return id(n)
      """
    Then the result should be, in any order, with relax comparison:
      | id(n)               |
      | "LaMarcus Aldridge" |
      | "Tony Parker"       |
    When executing query:
      """
      MATCH (n) WHERE id(n) IN ['LaMarcus Aldridge', 'Tony Parker', 'not_exist_vertex']
      return id(n), `tags`(n)
      """
    Then the result should be, in any order, with relax comparison:
      | id(n)               | tags(n)    |
      | "LaMarcus Aldridge" | ['player'] |
      | "Tony Parker"       | ['player'] |
    When executing query:
      """
      MATCH (start)-[e]-(end) WHERE id(start) IN ["Paul George", "not_exist_vertex"]
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | start           | e                                          | end                   |
      | ("Paul George") | [:like "Paul George"<-"Russell Westbrook"] | ("Russell Westbrook") |
      | ("Paul George") | [:like "Paul George"->"Russell Westbrook"] | ("Russell Westbrook") |
      | ("Paul George") | [:serve "Paul George"->"Pacers"]           | ("Pacers")            |
      | ("Paul George") | [:serve "Paul George"->"Thunders"]         | ("Thunders")          |

  Scenario: one step
    When executing query:
      """
      MATCH (v1) -[r]-> (v2)
      WHERE id(v1) == "LeBron James"
      RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order, with relax comparison:
      | Type    | Name        |
      | 'like'  | 'Ray Allen' |
      | 'serve' | 'Cavaliers' |
      | 'serve' | 'Heat'      |
      | 'serve' | 'Cavaliers' |
      | 'serve' | 'Lakers'    |
    When executing query:
      """
      MATCH (v1) -[r:serve|:like]-> (v2)
      WHERE id(v1) == "LeBron James"
      RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order, with relax comparison:
      | Type    | Name        |
      | 'like'  | 'Ray Allen' |
      | 'serve' | 'Cavaliers' |
      | 'serve' | 'Heat'      |
      | 'serve' | 'Cavaliers' |
      | 'serve' | 'Lakers'    |
    When executing query:
      """
      MATCH (v1) -[r:serve]-> (v2)
      WHERE id(v1) == "LeBron James"
      RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order, with relax comparison:
      | Type    | Name        |
      | 'serve' | 'Cavaliers' |
      | 'serve' | 'Heat'      |
      | 'serve' | 'Cavaliers' |
      | 'serve' | 'Lakers'    |
    When executing query:
      """
      MATCH (v1) -[r:serve]-> (v2 {name: "Cavaliers"})
      WHERE id(v1) == "LeBron James"
      RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order, with relax comparison:
      | Type    | Name        |
      | 'serve' | 'Cavaliers' |
      | 'serve' | 'Cavaliers' |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v2)
      WHERE id(v1) == "Danny Green"
      RETURN v1.name AS Name, v2.name AS Friend
      """
    Then the result should be, in any order, with relax comparison:
      | Name          | Friend            |
      | 'Danny Green' | 'LeBron James'    |
      | 'Danny Green' | 'Marco Belinelli' |
      | 'Danny Green' | 'Tim Duncan'      |
    When executing query:
      """
      MATCH (v1) <-[:like]- (v2)
      WHERE id(v1) == "Danny Green"
      RETURN v1.name AS Name, v2.name AS Friend
      """
    Then the result should be, in any order, with relax comparison:
      | Name          | Friend            |
      | 'Danny Green' | 'Dejounte Murray' |
      | 'Danny Green' | 'Marco Belinelli' |
    When executing query:
      """
      MATCH (v1) <-[:like]-> (v2)
      WHERE id(v1) == "Danny Green"
      RETURN v1.name AS Name, v2.name AS Friend
      """
    Then the result should be, in any order, with relax comparison:
      | Name          | Friend            |
      | 'Danny Green' | 'Dejounte Murray' |
      | 'Danny Green' | 'Marco Belinelli' |
      | 'Danny Green' | 'LeBron James'    |
      | 'Danny Green' | 'Marco Belinelli' |
      | 'Danny Green' | 'Tim Duncan'      |
    When executing query:
      """
      MATCH (v1) -[:like]- (v2)
      WHERE id(v1) == "Danny Green"
      RETURN v1.name AS Name, v2.name AS Friend
      """
    Then the result should be, in any order, with relax comparison:
      | Name          | Friend            |
      | 'Danny Green' | 'Dejounte Murray' |
      | 'Danny Green' | 'Marco Belinelli' |
      | 'Danny Green' | 'LeBron James'    |
      | 'Danny Green' | 'Marco Belinelli' |
      | 'Danny Green' | 'Tim Duncan'      |

  Scenario: two steps
    When executing query:
      """
      MATCH (v1) -[:like]-> (v2) -[:like]-> (v3)
      WHERE id(v1) == "Tim Duncan"
      RETURN v1.name AS Player, v2.name AS Friend, v3.name AS FoF
      """
    Then the result should be, in any order, with relax comparison:
      | Player       | Friend          | FoF                 |
      | 'Tim Duncan' | 'Manu Ginobili' | 'Tim Duncan'        |
      | 'Tim Duncan' | 'Tony Parker'   | 'LaMarcus Aldridge' |
      | 'Tim Duncan' | 'Tony Parker'   | 'Manu Ginobili'     |
      | 'Tim Duncan' | 'Tony Parker'   | 'Tim Duncan'        |

  Scenario: distinct
    When executing query:
      """
      MATCH (v1) -[:like]-> () -[:like]-> (v3)
      WHERE id(v1) == 'Dwyane Wade'
      RETURN v3.name AS Name
      """
    Then the result should be, in any order, with relax comparison:
      | Name              |
      | 'Carmelo Anthony' |
      | 'Dwyane Wade'     |
      | 'Dwyane Wade'     |
      | 'LeBron James'    |
      | 'LeBron James'    |
      | 'Chris Paul'      |
      | 'Ray Allen'       |
    When executing query:
      """
      MATCH (v1) -[:like]-> () -[:like]-> (v3)
      WHERE id(v1) == 'Dwyane Wade'
      RETURN DISTINCT v3.name AS Name
      """
    Then the result should be, in any order, with relax comparison:
      | Name              |
      | 'Carmelo Anthony' |
      | 'Dwyane Wade'     |
      | 'LeBron James'    |
      | 'Chris Paul'      |
      | 'Ray Allen'       |

  Scenario: order skip limit
    When executing query:
      """
      MATCH (v1) -[:like]-> (v)
      WHERE id(v1) == 'Dejounte Murray'
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      """
    Then the result should be, in any order, with relax comparison:
      | Name                | Age |
      | 'Tim Duncan'        | 42  |
      | 'Manu Ginobili'     | 41  |
      | 'Tony Parker'       | 36  |
      | 'LeBron James'      | 34  |
      | 'Chris Paul'        | 33  |
      | 'Marco Belinelli'   | 32  |
      | 'Danny Green'       | 31  |
      | 'Kevin Durant'      | 30  |
      | 'Russell Westbrook' | 30  |
      | 'James Harden'      | 29  |
      | 'Kyle Anderson'     | 25  |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v)
      WHERE id(v1) == 'Dejounte Murray'
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | Name            | Age |
      | 'Tim Duncan'    | 42  |
      | 'Manu Ginobili' | 41  |
      | 'Tony Parker'   | 36  |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v)
      WHERE id(v1) == 'Dejounte Murray'
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      SKIP 3
      """
    Then the result should be, in any order, with relax comparison:
      | Name                | Age |
      | 'LeBron James'      | 34  |
      | 'Chris Paul'        | 33  |
      | 'Marco Belinelli'   | 32  |
      | 'Danny Green'       | 31  |
      | 'Kevin Durant'      | 30  |
      | 'Russell Westbrook' | 30  |
      | 'James Harden'      | 29  |
      | 'Kyle Anderson'     | 25  |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v)
      WHERE id(v1) == 'Dejounte Murray'
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      SKIP 3
      LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | Name              | Age |
      | 'LeBron James'    | 34  |
      | 'Chris Paul'      | 33  |
      | 'Marco Belinelli' | 32  |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v)
      WHERE id(v1) == 'Dejounte Murray'
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      SKIP 11
      LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | Name | Age |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v)
      WHERE id(v1) == 'Dejounte Murray'
      RETURN v.name AS Name, v.age AS Age
      ORDER BY Age DESC, Name ASC
      LIMIT 0
      """
    Then the result should be, in any order, with relax comparison:
      | Name | Age |
    When executing query:
      """
      MATCH (v1) -[:like]-> (v)
      WHERE id(v1) == 'Dejounte Murray'
      RETURN v.name AS Name, v.age AS Age
      ORDER BY v.age DESC, v.name ASC
      LIMIT 0
      """
    Then a SemanticError should be raised at runtime: Only column name can be used as sort item

  Scenario: return path
    When executing query:
      """
      MATCH p = (n)
      WHERE id(n) == "Tony Parker"
      RETURN p,n
      """
    Then the result should be, in any order, with relax comparison:
      | p                 | n               |
      | <("Tony Parker")> | ("Tony Parker") |
    When executing query:
      """
      MATCH p = (n)-[:like]->(m)
      WHERE id(n) == "LeBron James"
      RETURN p, n.name, m.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                         | n.name         | m.name      |
      | <("LeBron James")-[:like]->("Ray Allen")> | "LeBron James" | "Ray Allen" |
    When executing query:
      """
      MATCH p = (n)<-[:like]-(m)
      WHERE id(n) == "LeBron James"
      RETURN p, n.name, m.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                               | n.name         | m.name            |
      | <("LeBron James")<-[:like]-("Carmelo Anthony")> | "LeBron James" | "Carmelo Anthony" |
      | <("LeBron James")<-[:like]-("Chris Paul")>      | "LeBron James" | "Chris Paul"      |
      | <("LeBron James")<-[:like]-("Danny Green")>     | "LeBron James" | "Danny Green"     |
      | <("LeBron James")<-[:like]-("Dejounte Murray")> | "LeBron James" | "Dejounte Murray" |
      | <("LeBron James")<-[:like]-("Dwyane Wade")>     | "LeBron James" | "Dwyane Wade"     |
      | <("LeBron James")<-[:like]-("Kyrie Irving")>    | "LeBron James" | "Kyrie Irving"    |
    When executing query:
      """
      MATCH p = (n)-[:like]-(m)
      WHERE id(n) == "LeBron James"
      RETURN p, n.name, m.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                               | n.name         | m.name            |
      | <("LeBron James")<-[:like]-("Carmelo Anthony")> | "LeBron James" | "Carmelo Anthony" |
      | <("LeBron James")<-[:like]-("Chris Paul")>      | "LeBron James" | "Chris Paul"      |
      | <("LeBron James")<-[:like]-("Danny Green")>     | "LeBron James" | "Danny Green"     |
      | <("LeBron James")<-[:like]-("Dejounte Murray")> | "LeBron James" | "Dejounte Murray" |
      | <("LeBron James")<-[:like]-("Dwyane Wade")>     | "LeBron James" | "Dwyane Wade"     |
      | <("LeBron James")<-[:like]-("Kyrie Irving")>    | "LeBron James" | "Kyrie Irving"    |
      | <("LeBron James")-[:like]->("Ray Allen")>       | "LeBron James" | "Ray Allen"       |
    When executing query:
      """
      MATCH p = (n)-[:like]->(m)-[:like]->(k)
      WHERE id(n) == "LeBron James"
      RETURN p, n.name, m.name, k.name
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                  | n.name         | m.name      | k.name        |
      | <("LeBron James")-[:like]->("Ray Allen")-[:like]->("Rajon Rondo")> | "LeBron James" | "Ray Allen" | "Rajon Rondo" |
    When executing query:
      """
      MATCH p = (n)-[:like]->()-[:like]->()
      WHERE id(n) == "LeBron James"
      RETURN *
      """
    Then the result should be, in any order, with relax comparison:
      | n                | p                                                                  |
      | ("LeBron James") | <("LeBron James")-[:like]->("Ray Allen")-[:like]->("Rajon Rondo")> |

  Scenario: hops m to n
    When executing query:
      """
      MATCH (n)-[e:serve*2..3{start_year: 2000}]-(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e | v |
    When executing query:
      """
      MATCH (n)-[e:like*2..3{likeness: 90}]-(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                  | v                  |
      | [[:like "Manu Ginobili"->"Tim Duncan"], [:like "Tiago Splitter"->"Manu Ginobili"]] | ("Tiago Splitter") |
    When executing query:
      """
      MATCH (n)-[e:serve*2..3{start_year: 2000}]->(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e | v |
    When executing query:
      """
      MATCH (n)-[e:like*2..3{likeness: 90}]->(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e | v |
    When executing query:
      """
      MATCH (n)<-[e:like*2..3{likeness: 90}]-(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                  | v                  |
      | [[:like "Manu Ginobili"->"Tim Duncan"], [:like "Tiago Splitter"->"Manu Ginobili"]] | ("Tiago Splitter") |
    When executing query:
      """
      MATCH (n)-[e:serve*2..3]-(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                          | v                     |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Paul Gasol"->"Spurs"@0],[:serve "Paul Gasol"->"Bucks"@0]]                       | ("Bucks")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Paul Gasol"->"Spurs"@0],[:serve "Paul Gasol"->"Bulls"@0]]                       | ("Bulls")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Paul Gasol"->"Spurs"@0],[:serve "Paul Gasol"->"Grizzlies"@0]]                   | ("Grizzlies")         |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Paul Gasol"->"Spurs"@0],[:serve "Paul Gasol"->"Lakers"@0]]                      | ("Lakers")            |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Cory Joseph"->"Spurs"@0],[:serve "Cory Joseph"->"Pacers"@0]]                    | ("Pacers")            |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Cory Joseph"->"Spurs"@0],[:serve "Cory Joseph"->"Raptors"@0]]                   | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"76ers"@0]]             | ("76ers")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"76ers"@0]]             | ("76ers")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"Bulls"@0]]             | ("Bulls")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"Bulls"@0]]             | ("Bulls")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"Hawks"@0]]             | ("Hawks")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"Hawks"@0]]             | ("Hawks")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"Hornets"@0]]           | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"Hornets"@0]]           | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"Kings"@0]]             | ("Kings")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"Kings"@0]]             | ("Kings")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"Raptors"@0]]           | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"Raptors"@0]]           | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"Spurs"@0]]             | ("Spurs")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"Warriors"@0]]          | ("Warriors")          |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"Warriors"@0]]          | ("Warriors")          |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"Hornets"@1]]           | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1],[:serve "Marco Belinelli"->"Hornets"@1]]           | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1]]             | ("Spurs")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tony Parker"->"Spurs"@0],[:serve "Tony Parker"->"Hornets"@0]]                   | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "David West"->"Spurs"@0],[:serve "David West"->"Hornets"@0]]                     | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "David West"->"Spurs"@0],[:serve "David West"->"Pacers"@0]]                      | ("Pacers")            |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "David West"->"Spurs"@0],[:serve "David West"->"Warriors"@0]]                    | ("Warriors")          |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Kyle Anderson"->"Spurs"@0],[:serve "Kyle Anderson"->"Grizzlies"@0]]             | ("Grizzlies")         |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tiago Splitter"->"Spurs"@0],[:serve "Tiago Splitter"->"76ers"@0]]               | ("76ers")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tiago Splitter"->"Spurs"@0],[:serve "Tiago Splitter"->"Hawks"@0]]               | ("Hawks")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Boris Diaw"->"Spurs"@0],[:serve "Boris Diaw"->"Hawks"@0]]                       | ("Hawks")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Boris Diaw"->"Spurs"@0],[:serve "Boris Diaw"->"Hornets"@0]]                     | ("Hornets")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Boris Diaw"->"Spurs"@0],[:serve "Boris Diaw"->"Jazz"@0]]                        | ("Jazz")              |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Boris Diaw"->"Spurs"@0],[:serve "Boris Diaw"->"Suns"@0]]                        | ("Suns")              |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Aron Baynes"->"Spurs"@0],[:serve "Aron Baynes"->"Celtics"@0]]                   | ("Celtics")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Aron Baynes"->"Spurs"@0],[:serve "Aron Baynes"->"Pistons"@0]]                   | ("Pistons")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Danny Green"->"Spurs"@0],[:serve "Danny Green"->"Cavaliers"@0]]                 | ("Cavaliers")         |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Danny Green"->"Spurs"@0],[:serve "Danny Green"->"Raptors"@0]]                   | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "LaMarcus Aldridge"->"Spurs"@0],[:serve "LaMarcus Aldridge"->"Trail Blazers"@0]] | ("Trail Blazers")     |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Rudy Gay"->"Spurs"@0],[:serve "Rudy Gay"->"Grizzlies"@0]]                       | ("Grizzlies")         |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Rudy Gay"->"Spurs"@0],[:serve "Rudy Gay"->"Kings"@0]]                           | ("Kings")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Rudy Gay"->"Spurs"@0],[:serve "Rudy Gay"->"Raptors"@0]]                         | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Jonathon Simmons"->"Spurs"@0],[:serve "Jonathon Simmons"->"76ers"@0]]           | ("76ers")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Jonathon Simmons"->"Spurs"@0],[:serve "Jonathon Simmons"->"Magic"@0]]           | ("Magic")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tracy McGrady"->"Spurs"@0],[:serve "Tracy McGrady"->"Magic"@0]]                 | ("Magic")             |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tracy McGrady"->"Spurs"@0],[:serve "Tracy McGrady"->"Raptors"@0]]               | ("Raptors")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tracy McGrady"->"Spurs"@0],[:serve "Tracy McGrady"->"Rockets"@0]]               | ("Rockets")           |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Aron Baynes"->"Spurs"@0]]                                                       | ("Aron Baynes")       |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Boris Diaw"->"Spurs"@0]]                                                        | ("Boris Diaw")        |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Cory Joseph"->"Spurs"@0]]                                                       | ("Cory Joseph")       |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Danny Green"->"Spurs"@0]]                                                       | ("Danny Green")       |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "David West"->"Spurs"@0]]                                                        | ("David West")        |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Dejounte Murray"->"Spurs"@0]]                                                   | ("Dejounte Murray")   |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Jonathon Simmons"->"Spurs"@0]]                                                  | ("Jonathon Simmons")  |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Kyle Anderson"->"Spurs"@0]]                                                     | ("Kyle Anderson")     |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "LaMarcus Aldridge"->"Spurs"@0]]                                                 | ("LaMarcus Aldridge") |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Manu Ginobili"->"Spurs"@0]]                                                     | ("Manu Ginobili")     |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@0]]                                                   | ("Marco Belinelli")   |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Paul Gasol"->"Spurs"@0]]                                                        | ("Paul Gasol")        |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Rudy Gay"->"Spurs"@0]]                                                          | ("Rudy Gay")          |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tiago Splitter"->"Spurs"@0]]                                                    | ("Tiago Splitter")    |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tony Parker"->"Spurs"@0]]                                                       | ("Tony Parker")       |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Tracy McGrady"->"Spurs"@0]]                                                     | ("Tracy McGrady")     |
      | [[:serve "Tim Duncan"->"Spurs"@0],[:serve "Marco Belinelli"->"Spurs"@1]]                                                   | ("Marco Belinelli")   |
    When executing query:
      """
      MATCH (n)-[e:serve*2..3]->(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e | v |
    When executing query:
      """
      MATCH (n)-[e:like*2..3]->(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                                 | v                     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]  | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]] | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]             | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0]]           | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]          | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]                                                 | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]                                              | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]                                                  | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                                                     | ("Tim Duncan")        |
    When executing query:
      """
      MATCH (n)-[e:like*2..3]-(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                                           | v                     |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"LeBron James"@0],[:like "Carmelo Anthony"->"LeBron James"@0]]         | ("Carmelo Anthony")   |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"LeBron James"@0],[:like "Carmelo Anthony"->"LeBron James"@0]]                 | ("Carmelo Anthony")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"LeBron James"@0],[:like "Chris Paul"->"LeBron James"@0]]              | ("Chris Paul")        |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"LeBron James"@0],[:like "Chris Paul"->"LeBron James"@0]]                      | ("Chris Paul")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"LeBron James"@0],[:like "Danny Green"->"LeBron James"@0]]             | ("Danny Green")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"LeBron James"@0],[:like "Dejounte Murray"->"LeBron James"@0]]                 | ("Dejounte Murray")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"LeBron James"@0],[:like "Dwyane Wade"->"LeBron James"@0]]             | ("Dwyane Wade")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"LeBron James"@0],[:like "Dwyane Wade"->"LeBron James"@0]]                     | ("Dwyane Wade")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"LeBron James"@0],[:like "Kyrie Irving"->"LeBron James"@0]]            | ("Kyrie Irving")      |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"LeBron James"@0],[:like "Kyrie Irving"->"LeBron James"@0]]                    | ("Kyrie Irving")      |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"LeBron James"@0],[:like "LeBron James"->"Ray Allen"@0]]               | ("Ray Allen")         |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"LeBron James"@0],[:like "LeBron James"->"Ray Allen"@0]]                       | ("Ray Allen")         |
      | [[:like "Tiago Splitter"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]         | ("Dejounte Murray")   |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]               | ("Dejounte Murray")   |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]               | ("Dejounte Murray")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0]]        | ("Tiago Splitter")    |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0]]                | ("Tiago Splitter")    |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0]]                | ("Tiago Splitter")    |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]            | ("Tim Duncan")        |
      | [[:like "Tiago Splitter"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]              | ("Tim Duncan")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]                    | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]                    | ("Tim Duncan")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]           | ("Tony Parker")       |
      | [[:like "Tiago Splitter"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]             | ("Tony Parker")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "Tiago Splitter"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]              | ("Tim Duncan")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]                    | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]                    | ("Tim Duncan")        |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Rudy Gay"->"LaMarcus Aldridge"@0],[:like "Tracy McGrady"->"Rudy Gay"@0]]               | ("Tracy McGrady")     |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0],[:like "James Harden"->"Russell Westbrook"@0]]  | ("James Harden")      |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0],[:like "Paul George"->"Russell Westbrook"@0]]   | ("Paul George")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0],[:like "Russell Westbrook"->"James Harden"@0]]  | ("James Harden")      |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0],[:like "Russell Westbrook"->"Paul George"@0]]   | ("Paul George")       |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0],[:like "Tiago Splitter"->"Tim Duncan"@0]]              | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0],[:like "Tiago Splitter"->"Tim Duncan"@0]]              | ("Tim Duncan")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Danny Green"->"Marco Belinelli"@0]]       | ("Danny Green")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0],[:like "Danny Green"->"Marco Belinelli"@0]]               | ("Danny Green")       |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Danny Green"->"Marco Belinelli"@0]]               | ("Danny Green")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Danny Green"->"Marco Belinelli"@0]]               | ("Danny Green")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]           | ("Dejounte Murray")   |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]           | ("Dejounte Murray")   |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]           | ("Dejounte Murray")   |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]           | ("Dejounte Murray")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Marco Belinelli"->"Danny Green"@0]]       | ("Danny Green")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0],[:like "Marco Belinelli"->"Danny Green"@0]]               | ("Danny Green")       |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Marco Belinelli"->"Danny Green"@0]]               | ("Danny Green")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Marco Belinelli"->"Danny Green"@0]]               | ("Danny Green")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]        | ("Tim Duncan")        |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]       | ("Tony Parker")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]               | ("Tony Parker")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]               | ("Tony Parker")       |
      | [[:like "Shaquile O'Neal"->"Tim Duncan"@0],[:like "Yao Ming"->"Shaquile O'Neal"@0],[:like "Yao Ming"->"Tracy McGrady"@0]]                   | ("Tracy McGrady")     |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Boris Diaw"->"Tony Parker"@0]]            | ("Boris Diaw")        |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Boris Diaw"->"Tony Parker"@0]]            | ("Boris Diaw")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Boris Diaw"->"Tony Parker"@0]]                | ("Boris Diaw")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Boris Diaw"->"Tony Parker"@0]]                | ("Boris Diaw")        |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Boris Diaw"->"Tony Parker"@0]]                    | ("Boris Diaw")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Boris Diaw"->"Tony Parker"@0]]                    | ("Boris Diaw")        |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]       | ("Dejounte Murray")   |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]       | ("Dejounte Murray")   |
      | [[:like "Boris Diaw"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]                     | ("Dejounte Murray")   |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]           | ("Dejounte Murray")   |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]               | ("Dejounte Murray")   |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]               | ("Dejounte Murray")   |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]     | ("LaMarcus Aldridge") |
      | [[:like "Boris Diaw"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]                   | ("LaMarcus Aldridge") |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]         | ("LaMarcus Aldridge") |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]         | ("LaMarcus Aldridge") |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]             | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]             | ("LaMarcus Aldridge") |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]       | ("Marco Belinelli")   |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]       | ("Marco Belinelli")   |
      | [[:like "Boris Diaw"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]                     | ("Marco Belinelli")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]           | ("Marco Belinelli")   |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]               | ("Marco Belinelli")   |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]               | ("Marco Belinelli")   |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Tim Duncan"->"Tony Parker"@0]]            | ("Tim Duncan")        |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Tim Duncan"->"Tony Parker"@0]]            | ("Tim Duncan")        |
      | [[:like "Boris Diaw"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "Tim Duncan"->"Tony Parker"@0]]                          | ("Tim Duncan")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Tim Duncan"->"Tony Parker"@0]]                | ("Tim Duncan")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Tim Duncan"->"Tony Parker"@0]]                | ("Tim Duncan")        |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tim Duncan"->"Tony Parker"@0]]                    | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tim Duncan"->"Tony Parker"@0]]                    | ("Tim Duncan")        |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]     | ("LaMarcus Aldridge") |
      | [[:like "Boris Diaw"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]                   | ("LaMarcus Aldridge") |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]         | ("LaMarcus Aldridge") |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]         | ("LaMarcus Aldridge") |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]             | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]             | ("LaMarcus Aldridge") |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]         | ("Manu Ginobili")     |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]         | ("Manu Ginobili")     |
      | [[:like "Boris Diaw"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]                       | ("Manu Ginobili")     |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]             | ("Manu Ginobili")     |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]             | ("Manu Ginobili")     |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Tony Parker"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "Boris Diaw"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                          | ("Tim Duncan")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                    | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                    | ("Tim Duncan")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Danny Green"@0]]           | ("Dejounte Murray")   |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0],[:like "Dejounte Murray"->"Danny Green"@0]]           | ("Dejounte Murray")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Marco Belinelli"->"Danny Green"@0]]           | ("Marco Belinelli")   |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0],[:like "Marco Belinelli"->"Danny Green"@0]]           | ("Marco Belinelli")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Danny Green"->"LeBron James"@0]]              | ("LeBron James")      |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0],[:like "Danny Green"->"LeBron James"@0]]              | ("LeBron James")      |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0],[:like "Danny Green"->"LeBron James"@0]]              | ("LeBron James")      |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Danny Green"->"Marco Belinelli"@0]]           | ("Marco Belinelli")   |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0],[:like "Danny Green"->"Marco Belinelli"@0]]           | ("Marco Belinelli")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Danny Green"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0],[:like "Danny Green"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0],[:like "Danny Green"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"James Harden"@0],[:like "Luka Doncic"->"James Harden"@0]]             | ("Luka Doncic")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"James Harden"@0],[:like "Russell Westbrook"->"James Harden"@0]]       | ("Russell Westbrook") |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"James Harden"@0],[:like "James Harden"->"Russell Westbrook"@0]]       | ("Russell Westbrook") |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Chris Paul"@0],[:like "Blake Griffin"->"Chris Paul"@0]]               | ("Blake Griffin")     |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Chris Paul"@0],[:like "Carmelo Anthony"->"Chris Paul"@0]]             | ("Carmelo Anthony")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Chris Paul"@0],[:like "Dwyane Wade"->"Chris Paul"@0]]                 | ("Dwyane Wade")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Chris Paul"@0],[:like "Chris Paul"->"Carmelo Anthony"@0]]             | ("Carmelo Anthony")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Chris Paul"@0],[:like "Chris Paul"->"Dwyane Wade"@0]]                 | ("Dwyane Wade")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Chris Paul"@0],[:like "Chris Paul"->"LeBron James"@0]]                | ("LeBron James")      |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Damian Lillard"->"LaMarcus Aldridge"@0]]        | ("Damian Lillard")    |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Damian Lillard"->"LaMarcus Aldridge"@0]]        | ("Damian Lillard")    |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Damian Lillard"->"LaMarcus Aldridge"@0]]        | ("Damian Lillard")    |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Damian Lillard"->"LaMarcus Aldridge"@0]]        | ("Damian Lillard")    |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Rudy Gay"->"LaMarcus Aldridge"@0]]              | ("Rudy Gay")          |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Rudy Gay"->"LaMarcus Aldridge"@0]]              | ("Rudy Gay")          |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Rudy Gay"->"LaMarcus Aldridge"@0]]              | ("Rudy Gay")          |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "Rudy Gay"->"LaMarcus Aldridge"@0]]              | ("Rudy Gay")          |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]           | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]           | ("Tony Parker")       |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]           | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]           | ("Tony Parker")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"Chris Paul"@0]]                | ("Chris Paul")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Chris Paul"@0]]        | ("Chris Paul")        |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Chris Paul"@0]]            | ("Chris Paul")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Chris Paul"@0]]            | ("Chris Paul")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Chris Paul"@0]]                | ("Chris Paul")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Chris Paul"@0]]                | ("Chris Paul")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Danny Green"@0]]       | ("Danny Green")       |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Danny Green"@0]]           | ("Danny Green")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Danny Green"@0]]           | ("Danny Green")       |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Danny Green"@0]]               | ("Danny Green")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Danny Green"@0]]               | ("Danny Green")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"James Harden"@0]]              | ("James Harden")      |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"James Harden"@0]]      | ("James Harden")      |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"James Harden"@0]]          | ("James Harden")      |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"James Harden"@0]]          | ("James Harden")      |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"James Harden"@0]]              | ("James Harden")      |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"James Harden"@0]]              | ("James Harden")      |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"Kevin Durant"@0]]              | ("Kevin Durant")      |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Kevin Durant"@0]]      | ("Kevin Durant")      |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Kevin Durant"@0]]          | ("Kevin Durant")      |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Kevin Durant"@0]]          | ("Kevin Durant")      |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Kevin Durant"@0]]              | ("Kevin Durant")      |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Kevin Durant"@0]]              | ("Kevin Durant")      |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"Kyle Anderson"@0]]             | ("Kyle Anderson")     |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Kyle Anderson"@0]]     | ("Kyle Anderson")     |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Kyle Anderson"@0]]         | ("Kyle Anderson")     |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Kyle Anderson"@0]]         | ("Kyle Anderson")     |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Kyle Anderson"@0]]             | ("Kyle Anderson")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Kyle Anderson"@0]]             | ("Kyle Anderson")     |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"LeBron James"@0]]              | ("LeBron James")      |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"LeBron James"@0]]      | ("LeBron James")      |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"LeBron James"@0]]          | ("LeBron James")      |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"LeBron James"@0]]          | ("LeBron James")      |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"LeBron James"@0]]              | ("LeBron James")      |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"LeBron James"@0]]              | ("LeBron James")      |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]             | ("Manu Ginobili")     |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]     | ("Manu Ginobili")     |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]             | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]             | ("Manu Ginobili")     |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]           | ("Marco Belinelli")   |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]       | ("Marco Belinelli")   |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]       | ("Marco Belinelli")   |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]           | ("Marco Belinelli")   |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]           | ("Marco Belinelli")   |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0]]         | ("Russell Westbrook") |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0]] | ("Russell Westbrook") |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0]]     | ("Russell Westbrook") |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0]]     | ("Russell Westbrook") |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0]]         | ("Russell Westbrook") |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0]]         | ("Russell Westbrook") |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]        | ("Tim Duncan")        |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]            | ("Tim Duncan")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]                | ("Tim Duncan")        |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]               | ("Tony Parker")       |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]       | ("Tony Parker")       |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]           | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]           | ("Tony Parker")       |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Aron Baynes"->"Tim Duncan"@0]]                     | ("Aron Baynes")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Aron Baynes"->"Tim Duncan"@0]]                     | ("Aron Baynes")       |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Aron Baynes"->"Tim Duncan"@0]]                         | ("Aron Baynes")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Aron Baynes"->"Tim Duncan"@0]]                         | ("Aron Baynes")       |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Boris Diaw"->"Tim Duncan"@0]]                      | ("Boris Diaw")        |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tim Duncan"@0]]                      | ("Boris Diaw")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Boris Diaw"->"Tim Duncan"@0]]                          | ("Boris Diaw")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tim Duncan"@0]]                          | ("Boris Diaw")        |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Danny Green"->"Tim Duncan"@0]]                     | ("Danny Green")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Danny Green"->"Tim Duncan"@0]]                     | ("Danny Green")       |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Danny Green"->"Tim Duncan"@0]]                         | ("Danny Green")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Danny Green"->"Tim Duncan"@0]]                         | ("Danny Green")       |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]                 | ("Dejounte Murray")   |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]                 | ("Dejounte Murray")   |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]                     | ("Dejounte Murray")   |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tim Duncan"@0]]                     | ("Dejounte Murray")   |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]               | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]               | ("LaMarcus Aldridge") |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]                   | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]                   | ("LaMarcus Aldridge") |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]                       | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]                       | ("Manu Ginobili")     |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]                 | ("Marco Belinelli")   |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]                 | ("Marco Belinelli")   |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]                     | ("Marco Belinelli")   |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tim Duncan"@0]]                     | ("Marco Belinelli")   |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Shaquile O'Neal"->"Tim Duncan"@0]]                 | ("Shaquile O'Neal")   |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Shaquile O'Neal"->"Tim Duncan"@0]]                 | ("Shaquile O'Neal")   |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Shaquile O'Neal"->"Tim Duncan"@0]]                     | ("Shaquile O'Neal")   |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Shaquile O'Neal"->"Tim Duncan"@0]]                     | ("Shaquile O'Neal")   |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tiago Splitter"->"Tim Duncan"@0]]                  | ("Tiago Splitter")    |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Tim Duncan"@0]]                  | ("Tiago Splitter")    |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tiago Splitter"->"Tim Duncan"@0]]                      | ("Tiago Splitter")    |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Tim Duncan"@0]]                      | ("Tiago Splitter")    |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                     | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                     | ("Tony Parker")       |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]                       | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]                       | ("Manu Ginobili")     |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tim Duncan"->"Tony Parker"@0]]                     | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0]]                     | ("Tony Parker")       |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "Boris Diaw"->"Tim Duncan"@0]]                          | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Boris Diaw"->"Tony Parker"@0],[:like "Boris Diaw"->"Tim Duncan"@0]]                          | ("Tim Duncan")        |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Damian Lillard"->"LaMarcus Aldridge"@0]]                                               | ("Damian Lillard")    |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Rudy Gay"->"LaMarcus Aldridge"@0]]                                                     | ("Rudy Gay")          |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]                                                  | ("Tony Parker")       |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]                                                  | ("Tony Parker")       |
      | [[:like "Boris Diaw"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0]]                                                                | ("Tony Parker")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Chris Paul"@0]]                                                       | ("Chris Paul")        |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0]]                                                      | ("Danny Green")       |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"James Harden"@0]]                                                     | ("James Harden")      |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Kevin Durant"@0]]                                                     | ("Kevin Durant")      |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Kyle Anderson"@0]]                                                    | ("Kyle Anderson")     |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"LeBron James"@0]]                                                     | ("LeBron James")      |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]                                                    | ("Manu Ginobili")     |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]                                                  | ("Marco Belinelli")   |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Russell Westbrook"@0]]                                                | ("Russell Westbrook") |
      | [[:like "Dejounte Murray"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]                                                      | ("Tony Parker")       |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Danny Green"@0]]                                                          | ("Dejounte Murray")   |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0]]                                                          | ("Marco Belinelli")   |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"LeBron James"@0]]                                                             | ("LeBron James")      |
      | [[:like "Danny Green"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0]]                                                          | ("Marco Belinelli")   |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Danny Green"->"Marco Belinelli"@0]]                                                      | ("Danny Green")       |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Marco Belinelli"@0]]                                                  | ("Dejounte Murray")   |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Danny Green"@0]]                                                      | ("Danny Green")       |
      | [[:like "Marco Belinelli"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]                                                      | ("Tony Parker")       |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]                                                      | ("Dejounte Murray")   |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Dejounte Murray"->"Manu Ginobili"@0]]                                                      | ("Dejounte Murray")   |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0]]                                                       | ("Tiago Splitter")    |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0]]                                                       | ("Tiago Splitter")    |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]                                                           | ("Tim Duncan")        |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]                                                          | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]                                                          | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]                                                           | ("Tim Duncan")        |
      | [[:like "Tiago Splitter"->"Tim Duncan"@0],[:like "Tiago Splitter"->"Manu Ginobili"@0]]                                                      | ("Manu Ginobili")     |
      | [[:like "Shaquile O'Neal"->"Tim Duncan"@0],[:like "Yao Ming"->"Shaquile O'Neal"@0]]                                                         | ("Yao Ming")          |
      | [[:like "Shaquile O'Neal"->"Tim Duncan"@0],[:like "Shaquile O'Neal"->"JaVale McGee"@0]]                                                     | ("JaVale McGee")      |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Boris Diaw"->"Tony Parker"@0]]                                                               | ("Boris Diaw")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Boris Diaw"->"Tony Parker"@0]]                                                               | ("Boris Diaw")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]                                                          | ("Dejounte Murray")   |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Dejounte Murray"->"Tony Parker"@0]]                                                          | ("Dejounte Murray")   |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]                                                        | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]                                                        | ("LaMarcus Aldridge") |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]                                                          | ("Marco Belinelli")   |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Marco Belinelli"->"Tony Parker"@0]]                                                          | ("Marco Belinelli")   |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0]]                                                               | ("Tim Duncan")        |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]                                                        | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]                                                        | ("LaMarcus Aldridge") |
      | [[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]                                                            | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]                                                            | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                                                               | ("Tim Duncan")        |
    When executing query:
      """
      MATCH (n)-[e:like*2..3{likeness: 90}]-(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                    | v                  |
      | [[:like "Manu Ginobili"->"Tim Duncan"@0 {likeness: 90}], [:like "Tiago Splitter"->"Manu Ginobili"@0 {likeness: 90}]] | ("Tiago Splitter") |
    When executing query:
      """
      MATCH (n)-[e:serve*2..3{start_year: 2000}]-(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e | v |
    When executing query:
      """
      MATCH (n)-[e:serve|like*2..3{likeness: 90}]->(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e | v |

  Scenario: prop limit
    When executing query:
      """
      MATCH (n)-[e:serve|like*2..3{likeness: 90}]-(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                      | v                  |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}], [:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]] | ("Tiago Splitter") |
    When executing query:
      """
      MATCH (n)-[e:serve|like*2..3]->(v)
      WHERE id(n) == "Tim Duncan"
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                                    | v                     |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:serve "Tim Duncan"->"Spurs"@0]]                   | ("Spurs")             |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:serve "Tim Duncan"->"Spurs"@0]]                       | ("Spurs")             |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0],[:like "Tim Duncan"->"Manu Ginobili"@0]]                | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0],[:like "Tim Duncan"->"Tony Parker"@0]]              | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:serve "Manu Ginobili"->"Spurs"@0]]                 | ("Spurs")             |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]             | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:serve "LaMarcus Aldridge"->"Spurs"@0]]         | ("Spurs")             |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:serve "LaMarcus Aldridge"->"Trail Blazers"@0]] | ("Trail Blazers")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tim Duncan"@0]]     | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0],[:like "LaMarcus Aldridge"->"Tony Parker"@0]]    | ("Tony Parker")       |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:serve "Manu Ginobili"->"Spurs"@0]]                                                        | ("Spurs")             |
      | [[:like "Tim Duncan"->"Manu Ginobili"@0],[:like "Manu Ginobili"->"Tim Duncan"@0]]                                                    | ("Tim Duncan")        |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:serve "Tony Parker"->"Hornets"@0]]                                                          | ("Hornets")           |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:serve "Tony Parker"->"Spurs"@0]]                                                            | ("Spurs")             |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"LaMarcus Aldridge"@0]]                                                 | ("LaMarcus Aldridge") |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Manu Ginobili"@0]]                                                     | ("Manu Ginobili")     |
      | [[:like "Tim Duncan"->"Tony Parker"@0],[:like "Tony Parker"->"Tim Duncan"@0]]                                                        | ("Tim Duncan")        |

  Scenario: count
    When executing query:
      """
      MATCH (:player{name: "Tim Duncan"})-[e:like*2..3]-(v)
      RETURN 1 | YIELD count(1)
      """
    Then the result should be, in any order, with relax comparison:
      | count(1) |
      | 292      |
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3]-(v)
      RETURN 1 | YIELD count(1)
      """
    Then the result should be, in any order, with relax comparison:
      | count(1) |
      | 927      |

  Scenario: count relationship
    When executing query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[e]->() RETURN type(e), count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | type(e)    | count(*) |
      | "serve"    | 1        |
      | "teammate" | 4        |
      | "like"     | 2        |

  Scenario: the referred vertex of id fn is not the first
    When executing query:
      """
      MATCH (v1)-[:like]->(v2:player)-[:serve]->(v3)
      WHERE id(v2) == 'Tim Duncan'
      RETURN v1, v3 |
      YIELD COUNT(*)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(*) |
      | 10       |
    When executing query:
      """
      MATCH (v1)-[:like]->(v2:player)-[:serve]->(v3)
      WHERE id(v3) == 'Spurs'
      RETURN v1 |
      YIELD COUNT(*)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(*) |
      | 34       |

  Scenario: [v2rc1 bug] https://discuss.nebula-graph.com.cn/t/topic/2646/15
    When executing query:
      """
      MATCH (v:player)-[e]-(v2)
      WHERE id(v)=='Marco Belinelli'
      RETURN v2
      """
    Then the result should be, in any order:
      | v2                                                                                                          |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("76ers" :team{name: "76ers"})                                                                              |
      | ("Bulls" :team{name: "Bulls"})                                                                              |
      | ("Hawks" :team{name: "Hawks"})                                                                              |
      | ("Hornets" :team{name: "Hornets"})                                                                          |
      | ("Kings" :team{name: "Kings"})                                                                              |
      | ("Raptors" :team{name: "Raptors"})                                                                          |
      | ("Spurs" :team{name: "Spurs"})                                                                              |
      | ("Warriors" :team{name: "Warriors"})                                                                        |
      | ("Hornets" :team{name: "Hornets"})                                                                          |
      | ("Spurs" :team{name: "Spurs"})                                                                              |

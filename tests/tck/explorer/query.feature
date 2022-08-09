Feature: explorer start

  Background:
    Given a graph with space named "nba"

  Scenario: inquire explorer
    When executing query:
     """
     show TAGS
     """
    Then the result should be, in any order, with relax comparison:
     | Name       |
     | "bachelor" |
     | "player"   |
     | "team"     |
    When executing query:
     """
     SHOW tag INDEXES
     """
    Then the result should be, in any order, with relax comparison:
     | Index Name          | By Tag     | Columns  |
     | "bachelor_index"    | "bachelor" | []       |
     | "player_age_index"  | "player"   | ["age"]  |
     | "player_name_index" | "player"   | ["name"] |
     | "team_name_index"   | "team"     | ["name"] |

    When executing query:
     """
     DESCRIBE tag INDEX `player_age_index`
     """
    Then the result should be, in any order, with relax comparison:
     | Field | Type    |
     | "age" | "int64" |


  Scenario: VID explorer
    When executing query:
      """
      MATCH (n) WHERE id(n) IN ["Amar'e Stoudemire", "Carmelo Anthony", "Danny Green", "Dejounte Murray","Dwight Howard"] RETURN n
      """
    Then the result should be, in any order, with relax comparison:
      | n                                                                 |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"}) |
      | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})     |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     |
      | ("Dwight Howard" :player{age: 33, name: "Dwight Howard"})         |
    When executing query:
     """
     MATCH (v)  RETURN id(v) limit 5
     """
    Then the result should be, in any order, with relax comparison:
      | id(v)               |
      | "Amar'e Stoudemire" |
      | "Carmelo Anthony"   |
      | "Danny Green"       |
      | "Dejounte Murray"   |
      | "Dwight Howard"     |
  Scenario: Tag explorer
    When executing query:
     """
     LOOKUP ON  `player` WHERE  `player`.`age` == 31     yield vertex as `vertex_` | LIMIT 1
     """
    Then the result should be, in any order, with relax comparison:
     | vertex_                                                   |
     | ("Stephen Curry" :player{age: 31, name: "Stephen Curry"}) |
    When executing query:
     """
     SHOW TAG INDEXES
     """
    Then the result should be, in any order, with relax comparison:
     | Index Name          | By Tag     | Columns  |
     | "bachelor_index"    | "bachelor" | []       |
     | "player_age_index"  | "player"   | ["age"]  |
     | "player_name_index" | "player"   | ["name"] |
     | "team_name_index"   | "team"     | ["name"] |
  Scenario: subgraph explorer
    When executing query:
     """
     GET SUBGRAPH 1 STEPS FROM "Danny Green" BOTH `like`, `serve`, `teammate`  YIELD VERTICES AS `vertices_`, EDGES AS `edges_`
     """
    Then the result should be, in any order, with relax comparison:
     | vertices_                                                                                                                                                                                       | edges_                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
     | [("Danny Green" :player{})]                                                                                                                                                                     | [[:teammate "Tim Duncan"->"Danny Green" @0 {}], [:serve "Danny Green"->"Cavaliers" @0 {}], [:serve "Danny Green"->"Raptors" @0 {}], [:serve "Danny Green"->"Spurs" @0 {}], [:like "Dejounte Murray"->"Danny Green" @0 {}], [:like "Marco Belinelli"->"Danny Green" @0 {}], [:like "Danny Green"->"LeBron James" @0 {}], [:like "Danny Green"->"Marco Belinelli" @0 {}], [:like "Danny Green"->"Tim Duncan" @0 {}]]                                                                                              |
     | [("Marco Belinelli" :player{}), ("Dejounte Murray" :player{}), ("Spurs" :team{}), ("Tim Duncan" :bachelor{} :player{}), ("Cavaliers" :team{}), ("Raptors" :team{}), ("LeBron James" :player{})] | [[:serve "Marco Belinelli"->"Raptors" @0 {}], [:serve "Marco Belinelli"->"Spurs" @0 {}], [:serve "Marco Belinelli"->"Spurs" @1 {}], [:like "Marco Belinelli"->"Tim Duncan" @0 {}], [:serve "Dejounte Murray"->"Spurs" @0 {}], [:like "Dejounte Murray"->"LeBron James" @0 {}], [:like "Dejounte Murray"->"Marco Belinelli" @0 {}], [:like "Dejounte Murray"->"Tim Duncan" @0 {}], [:serve "Tim Duncan"->"Spurs" @0 {}], [:serve "LeBron James"->"Cavaliers" @0 {}], [:serve "LeBron James"->"Cavaliers" @1 {}]] |

    When executing query:
     """
     MATCH (n) WHERE id(n) IN ["Danny Green", "Tim Duncan", "Cavaliers", "Raptors", "Spurs", "Dejounte Murray", "Marco Belinelli", "LeBron James"] RETURN n
     """
    Then the result should be, in any order, with relax comparison:
     | n                                                                                                           |
     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
     | ("Spurs" :team{name: "Spurs"})                                                                              |
     | ("Raptors" :team{name: "Raptors"})                                                                          |
     | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
     | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
     | ("Cavaliers" :team{name: "Cavaliers"})                                                                      |
     | ("LeBron James" :player{age: 34, name: "LeBron James"})                                                     |

    When executing query:
     """
     fetch prop on `teammate` "Tim Duncan"->"Danny Green"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                                         |
     | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}] |

    When executing query:
     """
     fetch prop on `like` "Dejounte Murray"->"Danny Green"@0, "Marco Belinelli"->"Danny Green"@0, "Danny Green"->"LeBron James"@0, "Danny Green"->"Marco Belinelli"@0, "Danny Green"->"Tim Duncan"@0, "Marco Belinelli"->"Tim Duncan"@0, "Dejounte Murray"->"LeBron James"@0, "Dejounte Murray"->"Marco Belinelli"@0, "Dejounte Murray"->"Tim Duncan"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                          |
     | [:like "Dejounte Murray"->"Danny Green" @0 {likeness: 99}]     |
     | [:like "Danny Green"->"LeBron James" @0 {likeness: 80}]        |
     | [:like "Danny Green"->"Marco Belinelli" @0 {likeness: 83}]     |
     | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]          |
     | [:like "Dejounte Murray"->"LeBron James" @0 {likeness: 99}]    |
     | [:like "Dejounte Murray"->"Marco Belinelli" @0 {likeness: 99}] |
     | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]      |
     | [:like "Marco Belinelli"->"Danny Green" @0 {likeness: 60}]     |
     | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]      | 
 
  Scenario: Expand_outflow explorer
    When executing query:
     """
     MATCH p=(v)-[e:`like`|:`serve`|:`teammate`*1]->(v2) WHERE id(v) IN ["Dejounte Murray"]     AND ALL(l IN e WHERE  l.likeness == 99) RETURN p LIMIT 100
     """
    Then the result should be, in any order, with relax comparison:
     | p                                                                                                                                                                                                     |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})>                                                   |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})>                                           |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                       |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("James Harden" :player{age: 29, name: "James Harden"})>                                                     |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Kevin Durant" :player{age: 30, name: "Kevin Durant"})>                                                     |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Danny Green" :player{age: 31, name: "Danny Green"})>                                                       |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Chris Paul" :player{age: 33, name: "Chris Paul"})>                                                         |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("LeBron James" :player{age: 34, name: "LeBron James"})>                                                     |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                   |
     | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})-[:like@0 {likeness: 99}]->("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})>                                               |
    
    When executing query:
     """
     MATCH (n) WHERE id(n) IN ["Dejounte Murray", "Kyle Anderson", "Russell Westbrook", "Tony Parker", "James Harden", "Kevin Durant", "Danny Green", "Chris Paul", "LeBron James", "Tim Duncan", "Manu Ginobili", "Marco Belinelli"] RETURN n
     """
    Then the result should be, in any order, with relax comparison:
     | n                                                                                                           |
     | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
     | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
     | ("Chris Paul" :player{age: 33, name: "Chris Paul"})                                                         |
     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
     | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
     | ("James Harden" :player{age: 29, name: "James Harden"})                                                     |
     | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
     | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})                                           |
     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
     | ("Kevin Durant" :player{age: 30, name: "Kevin Durant"})                                                     |
     | ("LeBron James" :player{age: 34, name: "LeBron James"})                                                     |

    When executing query:
     """
     fetch prop on `like` "Dejounte Murray"->"Kyle Anderson"@0, "Dejounte Murray"->"Russell Westbrook"@0, "Dejounte Murray"->"Tony Parker"@0, "Dejounte Murray"->"James Harden"@0, "Dejounte Murray"->"Kevin Durant"@0, "Dejounte Murray"->"Danny Green"@0, "Dejounte Murray"->"Chris Paul"@0, "Dejounte Murray"->"LeBron James"@0, "Dejounte Murray"->"Tim Duncan"@0, "Dejounte Murray"->"Manu Ginobili"@0, "Dejounte Murray"->"Marco Belinelli"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                            |
     | [:like "Dejounte Murray"->"Kyle Anderson" @0 {likeness: 99}]     |
     | [:like "Dejounte Murray"->"Russell Westbrook" @0 {likeness: 99}] |
     | [:like "Dejounte Murray"->"Tony Parker" @0 {likeness: 99}]       |
     | [:like "Dejounte Murray"->"James Harden" @0 {likeness: 99}]      |
     | [:like "Dejounte Murray"->"Kevin Durant" @0 {likeness: 99}]      |
     | [:like "Dejounte Murray"->"Danny Green" @0 {likeness: 99}]       |
     | [:like "Dejounte Murray"->"Chris Paul" @0 {likeness: 99}]        |
     | [:like "Dejounte Murray"->"LeBron James" @0 {likeness: 99}]      |
     | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]        |
     | [:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}]     |
     | [:like "Dejounte Murray"->"Marco Belinelli" @0 {likeness: 99}]   |

  Scenario: Expand_inflows explorer
    When executing query:
     """
     MATCH p=(v)<-[e:`like`|:`serve`|:`teammate`*1]-(v2) WHERE id(v) IN ["Tim Duncan"]     AND ALL(l IN e WHERE  l.likeness == 90) RETURN p LIMIT 100
     """
    Then the result should be, in any order, with relax comparison:
     | p                                                                                                                                                                                                 |
     | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 90}]-("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})> |
    When executing query:
     """
     MATCH (n) WHERE id(n) IN ["Manu Ginobili", "Tim Duncan"] RETURN n
     """
    Then the result should be, in any order, with relax comparison:
     | n                                                                                                           |
     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

    When executing query:
     """
     fetch prop on `like` "Manu Ginobili"->"Tim Duncan"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                   |
     | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}] |

  Scenario: Expand_two-way explorer
    When executing query:
     """
     MATCH p=(v)-[e:`like`|:`serve`|:`teammate`*1]-(v2) WHERE id(v) IN ["Tim Duncan"]      RETURN p LIMIT 5
     """
    Then the result should be, in any order, with relax comparison:
     | p                                                                                                                                                                                                         |
     | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 75}]-("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
     | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 99}]-("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})>     |
     | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 70}]-("Danny Green" :player{age: 31, name: "Danny Green"})>             |
     | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Aron Baynes" :player{age: 32, name: "Aron Baynes"})>             |
     | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Boris Diaw" :player{age: 36, name: "Boris Diaw"})>               |

    When executing query:
     """
     MATCH (n) WHERE id(n) IN ["Tony Parker", "Tim Duncan", "Tiago Splitter", "Shaquille O'Neal", "Marco Belinelli", "Manu Ginobili", "Boris Diaw", "Aron Baynes", "Spurs", "Danny Green", "Dejounte Murray", "LaMarcus Aldridge"] RETURN n
     """
    Then the result should be, in any order, with relax comparison:
     | n                                                                                                           |
     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
     | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
     | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
     | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
     | ("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})                                             |
     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
     | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
     | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
     | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
     | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
     | ("Spurs" :team{name: "Spurs"})                                                                              |

    When executing query:
     """
     fetch prop on `like` "LaMarcus Aldridge"->"Tim Duncan"@0, "Dejounte Murray"->"Tim Duncan"@0, "Danny Green"->"Tim Duncan"@0, "Aron Baynes"->"Tim Duncan"@0, "Boris Diaw"->"Tim Duncan"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                       |
     | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   |
     | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       |
     | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] |
     | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       |
     | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        |

  Scenario: neighbor explorer
    When executing query:
     """
     (GO FROM "Dejounte Murray" OVER *  YIELD dst(edge) as id  UNION GO FROM "Dejounte Murray"  OVER * REVERSELY YIELD src(edge) as id) INTERSECT(GO FROM "Danny Green" OVER *  YIELD dst(edge) as id  UNION GO FROM "Danny Green"  OVER * REVERSELY YIELD src(edge) as id)
     """
    Then the result should be, in any order, with relax comparison:
     | id                |
     | "Spurs"           |
     | "Tim Duncan"      |
     | "Marco Belinelli" |
     | "LeBron James"    |

    When executing query:
     """
     FIND ALL PATH FROM "Spurs", "Tim Duncan", "Marco Belinelli", "LeBron James" TO "Dejounte Murray", "Danny Green" OVER *  BIDIRECT UPTO 1 STEPS YIELD path as p | limit 5
     """
    Then the result should be, in any order, with relax comparison:
     | p                                                   |
     | <("Tim Duncan")<-[:like@0 {}]-("Danny Green")>      |
     | <("Tim Duncan")-[:teammate@0 {}]->("Danny Green")>  |
     | <("Spurs")<-[:serve@0 {}]-("Danny Green")>          |
     | <("LeBron James")<-[:like@0 {}]-("Danny Green")>    |
     | <("Marco Belinelli")<-[:like@0 {}]-("Danny Green")> |

    When executing query:
     """
     MATCH (n) WHERE id(n) IN ["Danny Green", "Tim Duncan", "Spurs", "LeBron James", "Marco Belinelli", "Dejounte Murray"] RETURN n
     """
    Then the result should be, in any order, with relax comparison:
     | n                                                                                                           |
     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
     | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
     | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
     | ("Spurs" :team{name: "Spurs"})                                                                              |
     | ("LeBron James" :player{age: 34, name: "LeBron James"})                                                     |
     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |

    When executing query:
     """
     fetch prop on `teammate` "Tim Duncan"->"Danny Green"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                                         |
     | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}] |

  Scenario: path explorer
    When executing query:
     """
     FIND ALL PATH FROM "Dejounte Murray" TO "Danny Green" over `like`, `serve`, `teammate`  UPTO 5 STEPS yield path as `_path` | LIMIT 5
     """
    Then the result should be, in any order, with relax comparison:
     | _path                                                                                                               |
     | <("Dejounte Murray")-[:like@0 {}]->("Danny Green")>                                                                 |
     | <("Dejounte Murray")-[:like@0 {}]->("Tim Duncan")-[:teammate@0 {}]->("Danny Green")>                                |
     | <("Dejounte Murray")-[:like@0 {}]->("Marco Belinelli")-[:like@0 {}]->("Danny Green")>                               |
     | <("Dejounte Murray")-[:like@0 {}]->("Danny Green")-[:like@0 {}]->("Marco Belinelli")-[:like@0 {}]->("Danny Green")> |
     | <("Dejounte Murray")-[:like@0 {}]->("Danny Green")-[:like@0 {}]->("Tim Duncan")-[:teammate@0 {}]->("Danny Green")>  |

    When executing query:
     """
     MATCH (n) WHERE id(n) IN ["Dejounte Murray", "Danny Green", "Tim Duncan", "Marco Belinelli"] RETURN n
     """
    Then the result should be, in any order, with relax comparison:
     | n                                                                                                           |
     | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
     | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |

    When executing query:
     """
     fetch prop on `teammate` "Tim Duncan"->"Danny Green"@0, "Tim Duncan"->"Danny Green"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                                         |
     | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}] |
     | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}] |

    When executing query:
     """
     fetch prop on `like` "Dejounte Murray"->"Danny Green"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                      |
     | [:like "Dejounte Murray"->"Danny Green" @0 {likeness: 99}] |

    When executing query:
     """
     fetch prop on `like` "Dejounte Murray"->"Danny Green"@0, "Dejounte Murray"->"Tim Duncan"@0, "Dejounte Murray"->"Marco Belinelli"@0, "Marco Belinelli"->"Danny Green"@0, "Dejounte Murray"->"Manu Ginobili"@0, "Manu Ginobili"->"Tim Duncan"@0, "Dejounte Murray"->"Manu Ginobili"@0 YIELD edge as `edge_`
     """
    Then the result should be, in any order, with relax comparison:
     | edge_                                                          |
     | [:like "Dejounte Murray"->"Danny Green" @0 {likeness: 99}]     |
     | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]      |
     | [:like "Dejounte Murray"->"Marco Belinelli" @0 {likeness: 99}] |
     | [:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}]   |
     | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]        |
     | [:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}]   |
     | [:like "Marco Belinelli"->"Danny Green" @0 {likeness: 60}]     |

  Scenario: Aerial_view explorer
    When executing query:
     """
     MATCH (v) RETURN id(v) as vid,tags(v) as tagsName limit 10
     """
    Then the result should be, in any order, with relax comparison:
     | vid                     | tagsName   |
     | "Amar'e Stoudemire"     | ["player"] |
     | "Carmelo Anthony"       | ["player"] |
     | "Danny Green"           | ["player"] |
     | "Dejounte Murray"       | ["player"] |
     | "Dwight Howard"         | ["player"] |
     | "Giannis Antetokounmpo" | ["player"] |
     | "Hornets"               | ["team"]   |
     | "James Harden"          | ["player"] |
     | "Joel Embiid"           | ["player"] |
     | "Kings"                 | ["team"]   |

    When executing query:
     """
     MATCH ()<-[e]-() RETURN src(e) as src, dst(e) as dst , rank(e) as rank limit 10
     """
    Then the result should be, in any order, with relax comparison:
     | src               | dst                 | rank |
     | "Steve Nash"      | "Amar'e Stoudemire" | 0    |
     | "Chris Paul"      | "Carmelo Anthony"   | 0    |
     | "Dwyane Wade"     | "Carmelo Anthony"   | 0    |
     | "Tim Duncan"      | "Danny Green"       | 0    |
     | "Dejounte Murray" | "Danny Green"       | 0    |
     | "Marco Belinelli" | "Danny Green"       | 0    |
     | "Boris Diaw"      | "Hornets"           | 0    |
     | "Chris Paul"      | "Hornets"           | 0    |
     | "David West"      | "Hornets"           | 0    |
     | "Dwight Howard"   | "Hornets"           | 0    |

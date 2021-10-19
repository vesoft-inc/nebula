# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Go Yield Vertex And Edge Sentence

  Background:
    Given a graph with space named "nba"

  Scenario: one step
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD edge as e, properties(edge) as props, concat(src(edge), " like ", dst(edge), " @ ", properties($$).name, " # ", properties($^).age) as result
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                       | props          | result                                               |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] | {likeness: 95} | "Tim Duncan like Manu Ginobili @ Manu Ginobili # 42" |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   | {likeness: 95} | "Tim Duncan like Tony Parker @ Tony Parker # 42"     |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve Yield src(edge) as src, dst(edge) as dst, type(edge) as type, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | src          | dst     | type    | e                                                                    |
      | "Tim Duncan" | "Spurs" | "serve" | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}] |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD $^.player.name as name, $^.player.age as age, $^ as src, $$ as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | name         | age | src                                                                                                         | dst                                                       | e                                                       |
      | "Tim Duncan" | 42  | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | "Tim Duncan" | 42  | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    When executing query:
      """
      GO FROM "Tim Duncan", "Tony Parker" OVER like YIELD $^.player.name as name, $^.player.age as age, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | name          | age | dst                                                                                                         |
      | "Tim Duncan"  | 42  | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "Tim Duncan"  | 42  | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | "Tony Parker" | 36  | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | "Tony Parker" | 36  | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "Tony Parker" | 36  | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      GO FROM "Tim Duncan", "Tim Duncan" OVER * YIELD $$ as dst, type(edge) as type
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                               | type       |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         | "like"     |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             | "like"     |
      | ("Spurs" :team{name: "Spurs"})                                    | "serve"    |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             | "teammate" |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) | "teammate" |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         | "teammate" |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             | "teammate" |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         | "like"     |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             | "like"     |
      | ("Spurs" :team{name: "Spurs"})                                    | "serve"    |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             | "teammate" |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) | "teammate" |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         | "teammate" |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             | "teammate" |
    When executing query:
      """
      YIELD "Tim Duncan" as vid | GO FROM $-.vid OVER serve YIELD $^ as src, $$ as dst, dst(edge) as id
      """
    Then the result should be, in any order, with relax comparison:
      | src                                                                                                         | dst                            | id      |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Spurs" :team{name: "Spurs"}) | "Spurs" |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name, edge as e, properties(edge) as props
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name | e                                                                      | props                              |
      | "Boris Diaw"   | 2003             | 2005           | "Hawks"      | [:serve "Boris Diaw"->"Hawks" @0 {end_year: 2005, start_year: 2003}]   | {end_year: 2005, start_year: 2003} |
      | "Boris Diaw"   | 2008             | 2012           | "Hornets"    | [:serve "Boris Diaw"->"Hornets" @0 {end_year: 2012, start_year: 2008}] | {end_year: 2012, start_year: 2008} |
      | "Boris Diaw"   | 2016             | 2017           | "Jazz"       | [:serve "Boris Diaw"->"Jazz" @0 {end_year: 2017, start_year: 2016}]    | {end_year: 2017, start_year: 2016} |
      | "Boris Diaw"   | 2012             | 2016           | "Spurs"      | [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]   | {end_year: 2016, start_year: 2012} |
      | "Boris Diaw"   | 2005             | 2008           | "Suns"       | [:serve "Boris Diaw"->"Suns" @0 {end_year: 2008, start_year: 2005}]    | {end_year: 2008, start_year: 2005} |
    When executing query:
      """
      GO FROM "Rajon Rondo" OVER serve WHERE serve.start_year >= 2013 AND serve.end_year <= 2018
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name, rank(edge) as rank, dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name | rank | dst         |
      | "Rajon Rondo"  | 2014             | 2015           | "Mavericks"  | 0    | "Mavericks" |
      | "Rajon Rondo"  | 2015             | 2016           | "Kings"      | 0    | "Kings"     |
      | "Rajon Rondo"  | 2016             | 2017           | "Bulls"      | 0    | "Bulls"     |
      | "Rajon Rondo"  | 2017             | 2018           | "Pelicans"   | 0    | "Pelicans"  |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER * YIELD dst(edge) as id |
      GO FROM $-.id OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER serve YIELD $$ as dstnode
      """
    Then the result should be, in any order, with relax comparison:
      | dstnode                                        |
      | ("Hornets" :team{name: "Hornets"})             |
      | ("Spurs" :team{name: "Spurs"})                 |
      | ("Spurs" :team{name: "Spurs"})                 |
      | ("Trail Blazers" :team{name: "Trail Blazers"}) |
      | ("Spurs" :team{name: "Spurs"})                 |
      | ("Spurs" :team{name: "Spurs"})                 |
      | ("Spurs" :team{name: "Spurs"})                 |
    When executing query:
      """
      GO FROM 'Thunders' OVER serve REVERSELY YIELD $^ as src,tags($^), id($^), id($$), $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | src                                  | tags($^) | id($^)     | id($$)              | dst                                                               |
      | ("Thunders" :team{name: "Thunders"}) | ["team"] | "Thunders" | "Carmelo Anthony"   | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})     |
      | ("Thunders" :team{name: "Thunders"}) | ["team"] | "Thunders" | "James Harden"      | ("James Harden" :player{age: 29, name: "James Harden"})           |
      | ("Thunders" :team{name: "Thunders"}) | ["team"] | "Thunders" | "Kevin Durant"      | ("Kevin Durant" :player{age: 30, name: "Kevin Durant"})           |
      | ("Thunders" :team{name: "Thunders"}) | ["team"] | "Thunders" | "Paul George"       | ("Paul George" :player{age: 28, name: "Paul George"})             |
      | ("Thunders" :team{name: "Thunders"}) | ["team"] | "Thunders" | "Ray Allen"         | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                 |
      | ("Thunders" :team{name: "Thunders"}) | ["team"] | "Thunders" | "Russell Westbrook" | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER serve YIELD distinct edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                   |
      | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]             |
      | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]               |
      | [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]         |
      | [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}] |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                |
      | [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]             |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like YIELD id($$) as id |
      GO FROM $-.id OVER like YIELD id($$) as id |
      GO FROM $-.id OVER serve YIELD distinct edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                   |
      | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]             |
      | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]               |
      | [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]         |
      | [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}] |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                |
      | [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]             |
    When executing query:
      """
      GO FROM "No Exist Vertex Id" OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER serve YIELD $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst |

  Scenario: In expression
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst IN ['Tim Duncan', 'Danny Green'] YIELD $$ as dst, tags($$), tags($^)
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                                                                         | tags($$)               | tags($^)   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ["bachelor", "player"] | ["player"] |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst IN ['Tim Duncan', 'Danny Green'] YIELD $^ as src
      """
    Then the result should be, in any order, with relax comparison:
      | src                                                   |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"}) |

  Scenario: assignment simple
    When executing query:
      """
      $var = GO FROM "Tracy McGrady" OVER like YIELD dst(edge) as id;
      GO FROM $var.id OVER like YIELD $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                               |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})         |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |

  Scenario: assignment pipe
    When executing query:
      """
      $var = (GO FROM "Tracy McGrady" OVER like YIELD dst(edge) as id | GO FROM $-.id OVER like YIELD dst(edge) as id);
      GO FROM $var.id OVER like YIELD dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst           |
      | "Kobe Bryant" |
      | "Grant Hill"  |
      | "Rudy Gay"    |
      | "Tony Parker" |
      | "Tim Duncan"  |

  Scenario: pipe only yield input prop
    When executing query:
      """
      GO FROM "Tracy McGrady" OVER like YIELD dst(edge) as vid |
      GO FROM $-.vid OVER like YIELD src(edge) as id
      """
    Then the result should be, in any order, with relax comparison:
      | id           |
      | "Grant Hill" |
      | "Rudy Gay"   |

  Scenario: pipe only yield constant
    When executing query:
      """
      GO FROM "Tracy McGrady" OVER like YIELD dst(edge) as vid |
      GO FROM $-.vid OVER like YIELD 3
      """
    Then the result should be, in any order, with relax comparison:
      | 3 |
      | 3 |
      | 3 |

  Scenario: assignment empty result
    When executing query:
      """
      $var = GO FROM "-1" OVER like YIELD dst(edge) as id;
      GO FROM $var.id OVER like YIELD dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst |

  Scenario: variable undefined
    When executing query:
      """
      GO FROM $var OVER like YIELD edge as e
      """
    Then a SyntaxError should be raised at runtime: syntax error near `OVER'

  Scenario: distinct map and set
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER serve YIELD DISTINCT dst(edge) as dst, edge as e, properties(edge) as props
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | e                                                                                   | props                              |
      | "Spurs"         | [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]             | {end_year: 2018, start_year: 2002} |
      | "Spurs"         | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                | {end_year: 2016, start_year: 1997} |
      | "Hornets"       | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]             | {end_year: 2019, start_year: 2018} |
      | "Spurs"         | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]               | {end_year: 2018, start_year: 1999} |
      | "Spurs"         | [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]         | {end_year: 2019, start_year: 2015} |
      | "Trail Blazers" | [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}] | {end_year: 2015, start_year: 2006} |
    When executing query:
      """
      GO 2 STEPS FROM "Tim Duncan" OVER like YIELD dst(edge) as id |
      YIELD DISTINCT collect($-.id) as a, collect_set($-.id) as b
      """
    Then the result should be, in any order, with relax comparison:
      | a                                                                  | b                                                    |
      | ["Tim Duncan", "LaMarcus Aldridge", "Manu Ginobili", "Tim Duncan"] | {"Manu Ginobili", "LaMarcus Aldridge", "Tim Duncan"} |

  Scenario: distinct
    When executing query:
      """
      GO 2 STEPS FROM "Tony Parker" OVER like YIELD DISTINCT dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst             |
      | "Tim Duncan"    |
      | "Tony Parker"   |
      | "Manu Ginobili" |

  Scenario: vertex noexist
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e |
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD $^ as src, $$ as dst, properties(edge) as props
      """
    Then the result should be, in any order, with relax comparison:
      | src | dst | props |

  Scenario: empty input
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER serve YIELD dst(edge) as id
      """
    Then the result should be, in any order, with relax comparison:
      | id |
    When executing query:
      """
      GO FROM "NON EXIST VERTEX ID" OVER serve YIELD dst(edge) as id, serve.start_year as start |
      YIELD $-.id as id WHERE $-.start > 20000 |
      Go FROM $-.id over serve YIELD dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst |

  Scenario: multi edges over all
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER * REVERSELY YIELD src(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst               |
      | "James Harden"    |
      | "Dejounte Murray" |
      | "Paul George"     |
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER * REVERSELY YIELD dst(edge) as src
      """
    Then the result should be, in any order, with relax comparison:
      | src                 |
      | "Russell Westbrook" |
      | "Russell Westbrook" |
      | "Russell Westbrook" |
    When executing query:
      """
      GO FROM "Dirk Nowitzki" OVER * YIELD dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst           |
      | "Dwyane Wade" |
      | "Jason Kidd"  |
      | "Steve Nash"  |
      | "Mavericks"   |
    When executing query:
      """
      GO FROM "Paul Gasol" OVER * YIELD $$ as node
      """
    Then the result should be, in any order, with relax comparison:
      | node                                                  |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"}) |
      | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"})   |
      | ("Bucks" :team{name: "Bucks"})                        |
      | ("Bulls" :team{name: "Bulls"})                        |
      | ("Grizzlies" :team{name: "Grizzlies"})                |
      | ("Lakers" :team{name: "Lakers"})                      |
      | ("Spurs" :team{name: "Spurs"})                        |
    When executing query:
      """
      GO FROM "Paul Gasol" OVER *
      WHERE $$.player.name IS NOT EMPTY
      YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                     |
      | [:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}] |
      | [:like "Paul Gasol"->"Marc Gasol" @0 {likeness: 99}]  |
    When executing query:
      """
      GO FROM "Paul Gasol" OVER *
      WHERE $$.player.name IS EMPTY
      YIELD type(edge) as type
      """
    Then the result should be, in any order, with relax comparison:
      | type    |
      | "serve" |
      | "serve" |
      | "serve" |
      | "serve" |
      | "serve" |
    When executing query:
      """
      GO FROM "Manu Ginobili" OVER * REVERSELY YIELD like.likeness, teammate.start_year, serve.start_year, $$.player.name, type(edge) as type
      """
    Then the result should be, in any order, with relax comparison:
      | like.likeness | teammate.start_year | serve.start_year | $$.player.name    | type       |
      | 95            | EMPTY               | EMPTY            | "Tim Duncan"      | "like"     |
      | 95            | EMPTY               | EMPTY            | "Tony Parker"     | "like"     |
      | 90            | EMPTY               | EMPTY            | "Tiago Splitter"  | "like"     |
      | 99            | EMPTY               | EMPTY            | "Dejounte Murray" | "like"     |
      | EMPTY         | 2002                | EMPTY            | "Tim Duncan"      | "teammate" |
      | EMPTY         | 2002                | EMPTY            | "Tony Parker"     | "teammate" |
    When executing query:
      """
      GO FROM "LaMarcus Aldridge" OVER * YIELD $$.team.name, $$.player.name, $$ as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | $$.team.name    | $$.player.name | dst                                                                                                         | e                                                                                   |
      | "Trail Blazers" | EMPTY          | ("Trail Blazers" :team{name: "Trail Blazers"})                                                              | [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}] |
      | EMPTY           | "Tim Duncan"   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]                         |
      | EMPTY           | "Tony Parker"  | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]                        |
      | "Spurs"         | EMPTY          | ("Spurs" :team{name: "Spurs"})                                                                              | [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]         |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER * YIELD like._dst as id |
      GO FROM $-.id OVER like YIELD like._dst as id |
      GO FROM $-.id OVER serve YIELD edge as e, properties(edge) as props
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                   | props                              |
      | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]             | {end_year: 2019, start_year: 2018} |
      | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]               | {end_year: 2018, start_year: 1999} |
      | [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]         | {end_year: 2019, start_year: 2015} |
      | [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}] | {end_year: 2015, start_year: 2006} |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                | {end_year: 2016, start_year: 1997} |
      | [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]             | {end_year: 2018, start_year: 2002} |
      | [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]             | {end_year: 2018, start_year: 2002} |

  Scenario: edge type
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER serve, like REVERSELY YIELD dst(edge) as dst, src(edge) as src, type(edge) as type
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 | src               | type   |
      | "Russell Westbrook" | "Dejounte Murray" | "like" |
      | "Russell Westbrook" | "James Harden"    | "like" |
      | "Russell Westbrook" | "Paul George"     | "like" |

  Scenario: multi edges
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER serve, like  YIELD properties(edge) as props, type(edge) as type
      """
    Then the result should be, in any order, with relax comparison:
      | props                              | type    |
      | {end_year: 2019, start_year: 2008} | "serve" |
      | {likeness: 90}                     | "like"  |
      | {likeness: 90}                     | "like"  |
    When executing query:
      """
      GO FROM "Shaquile O\'Neal" OVER serve, like YIELD dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst            |
      | "Cavaliers"    |
      | "Celtics"      |
      | "Heat"         |
      | "Lakers"       |
      | "Magic"        |
      | "Suns"         |
      | "JaVale McGee" |
      | "Tim Duncan"   |
    When executing query:
      """
      GO FROM 'Russell Westbrook' OVER serve, like YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                              |
      | [:serve "Russell Westbrook"->"Thunders" @0 {end_year: 2019, start_year: 2008}] |
      | [:like "Russell Westbrook"->"James Harden" @0 {likeness: 90}]                  |
      | [:like "Russell Westbrook"->"Paul George" @0 {likeness: 90}]                   |
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER serve, like REVERSELY YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                |
      | [:like "Dejounte Murray"->"Russell Westbrook" @0 {likeness: 99}] |
      | [:like "James Harden"->"Russell Westbrook" @0 {likeness: 80}]    |
      | [:like "Paul George"->"Russell Westbrook" @0 {likeness: 95}]     |
    When executing query:
      """
      GO FROM "Manu Ginobili" OVER like, teammate REVERSELY YIELD $^ as src, edge as e, $$ as dst, $$.player.name, $^.player.age
      """
    Then the result should be, in any order, with relax comparison:
      | src                                                       | e                                                                                | dst                                                                                                         | $$.player.name    | $^.player.age |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) | [:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}]                     | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               | "Dejounte Murray" | 41            |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) | [:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]                      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 | "Tiago Splitter"  | 41            |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]                          | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | "Tim Duncan"      | 41            |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                         | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | "Tony Parker"     | 41            |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) | [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]  | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | "Tim Duncan"      | 41            |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | "Tony Parker"     | 41            |

  Scenario: multi edges with filter
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER serve, like WHERE serve.start_year > 2000 YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                              |
      | [:serve "Russell Westbrook"->"Thunders" @0 {end_year: 2019, start_year: 2008}] |
    When executing query:
      """
      GO FROM "Manu Ginobili" OVER like, teammate REVERSELY WHERE like.likeness > 90 YIELD $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                                                                         |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      GO FROM "Manu Ginobili" OVER * WHERE $$.player.age > 30 or $$.team.name not starts with "Rockets" YIELD DISTINCT $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                                                                         |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Spurs" :team{name: "Spurs"})                                                                              |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
    When executing query:
      """
      GO FROM "Manu Ginobili" OVER like, teammate REVERSELY WHERE $$.player.age > 30 and $$.player.age < 40 YIELD DISTINCT $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                         |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})       |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"}) |

  Scenario: reference pipe in yield and where
    When executing query:
      """
      GO FROM 'Tim Duncan', 'Chris Paul' OVER like YIELD $^.player.name AS name, dst(edge) AS id |
      GO FROM $-.id OVER like YIELD $-.name, dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | $-.name      | dst                 |
      | "Tim Duncan" | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Manu Ginobili"     |
      | "Tim Duncan" | "Tim Duncan"        |
      | "Chris Paul" | "Chris Paul"        |
      | "Chris Paul" | "Dwyane Wade"       |
      | "Chris Paul" | "LeBron James"      |
      | "Chris Paul" | "Ray Allen"         |
      | "Tim Duncan" | "Tim Duncan"        |
      | "Chris Paul" | "Carmelo Anthony"   |
      | "Chris Paul" | "Chris Paul"        |
      | "Chris Paul" | "LeBron James"      |
    When executing query:
      """
      GO FROM 'Tim Duncan', 'Chris Paul' OVER like YIELD $^.player.name AS name, dst(edge) AS id |
      GO FROM $-.id OVER like WHERE $-.name != $$.player.name YIELD $-.name, $^.player.name, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | $-.name      | $^.player.name    | dst                                                               |
      | "Tim Duncan" | "Tony Parker"     | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | "Tim Duncan" | "Tony Parker"     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | "Chris Paul" | "Dwyane Wade"     | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
      | "Chris Paul" | "Dwyane Wade"     | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})     |
      | "Chris Paul" | "Carmelo Anthony" | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})             |
      | "Chris Paul" | "Carmelo Anthony" | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
      | "Chris Paul" | "LeBron James"    | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                 |
    When executing query:
      """
      GO FROM 'Yao Ming' OVER * YIELD $^ as src, id($$) AS id |
      GO FROM $-.id OVER * YIELD $-.src, id($$) as id
      """
    Then the result should be, in any order, with relax comparison:
      | $-.src                                          | id             |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "JaVale McGee" |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Tim Duncan"   |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Cavaliers"    |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Celtics"      |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Heat"         |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Lakers"       |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Magic"        |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Suns"         |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Grant Hill"   |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Kobe Bryant"  |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Rudy Gay"     |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Magic"        |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Raptors"      |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Rockets"      |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"}) | "Spurs"        |

  @skip
  Scenario: all prop(reson = $-.* over * $var.* not implement)
    When executing query:
      """
      GO FROM 'Tim Duncan', 'Chris Paul' OVER like YIELD $^.player.name AS name, like._dst AS id
      | GO FROM $-.id OVER like YIELD $-.*, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $-.*         | $^.player.name    | $$.player.name      |
      | "Tim Duncan" | "Manu Ginobili"   | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"     | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"     | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"     | "Tim Duncan"        |
      | "Chris Paul" | "LeBron James"    | "Ray Allen"         |
      | "Chris Paul" | "Carmelo Anthony" | "Chris Paul"        |
      | "Chris Paul" | "Carmelo Anthony" | "LeBron James"      |
      | "Chris Paul" | "Carmelo Anthony" | "Dwyane Wade"       |
      | "Chris Paul" | "Dwyane Wade"     | "Chris Paul"        |
      | "Chris Paul" | "Dwyane Wade"     | "LeBron James"      |
      | "Chris Paul" | "Dwyane Wade"     | "Carmelo Anthony"   |
    When executing query:
      """
      $var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like YIELD $^.player.name AS name, like._dst AS id;
      GO FROM $var.id OVER like YIELD $var.*, $^.player.name, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | $var.*       | $^.player.name    | $$.player.name      |
      | "Tim Duncan" | "Manu Ginobili"   | "Tim Duncan"        |
      | "Tim Duncan" | "Tony Parker"     | "LaMarcus Aldridge" |
      | "Tim Duncan" | "Tony Parker"     | "Manu Ginobili"     |
      | "Tim Duncan" | "Tony Parker"     | "Tim Duncan"        |
      | "Chris Paul" | "LeBron James"    | "Ray Allen"         |
      | "Chris Paul" | "Carmelo Anthony" | "Chris Paul"        |
      | "Chris Paul" | "Carmelo Anthony" | "LeBron James"      |
      | "Chris Paul" | "Carmelo Anthony" | "Dwyane Wade"       |
      | "Chris Paul" | "Dwyane Wade"     | "Chris Paul"        |
      | "Chris Paul" | "Dwyane Wade"     | "LeBron James"      |
      | "Chris Paul" | "Dwyane Wade"     | "Carmelo Anthony"   |

  Scenario: reference variable in yield and where
    When executing query:
      """
      $var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like YIELD $^.player.name AS name, like._dst AS id;
      GO FROM $var.id OVER like YIELD $var.name, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | $var.name    | e                                                            |
      | "Tim Duncan" | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | "Tim Duncan" | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | "Tim Duncan" | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | "Chris Paul" | [:like "Carmelo Anthony"->"Chris Paul" @0 {likeness: 90}]    |
      | "Chris Paul" | [:like "Carmelo Anthony"->"Dwyane Wade" @0 {likeness: 90}]   |
      | "Chris Paul" | [:like "Carmelo Anthony"->"LeBron James" @0 {likeness: 90}]  |
      | "Chris Paul" | [:like "LeBron James"->"Ray Allen" @0 {likeness: 100}]       |
      | "Tim Duncan" | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Chris Paul" | [:like "Dwyane Wade"->"Carmelo Anthony" @0 {likeness: 90}]   |
      | "Chris Paul" | [:like "Dwyane Wade"->"Chris Paul" @0 {likeness: 90}]        |
      | "Chris Paul" | [:like "Dwyane Wade"->"LeBron James" @0 {likeness: 90}]      |
    When executing query:
      """
      $var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like YIELD $^.player.name AS name, like._dst AS id;
      GO FROM $var.id OVER like  WHERE $var.name != $$.player.name YIELD $var.name, properties(edge) as props
      """
    Then the result should be, in any order, with relax comparison:
      | $var.name    | props           |
      | "Tim Duncan" | {likeness: 90}  |
      | "Tim Duncan" | {likeness: 95}  |
      | "Chris Paul" | {likeness: 90}  |
      | "Chris Paul" | {likeness: 90}  |
      | "Chris Paul" | {likeness: 90}  |
      | "Chris Paul" | {likeness: 90}  |
      | "Chris Paul" | {likeness: 100} |
    When executing query:
      """
      $var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like YIELD $^.player.name AS name, id($$) AS id;
      GO FROM $var.id OVER like  WHERE $var.name != $$.player.name YIELD $var.name, properties(edge) as props
      """
    Then the result should be, in any order, with relax comparison:
      | $var.name    | props           |
      | "Tim Duncan" | {likeness: 90}  |
      | "Tim Duncan" | {likeness: 95}  |
      | "Chris Paul" | {likeness: 90}  |
      | "Chris Paul" | {likeness: 90}  |
      | "Chris Paul" | {likeness: 90}  |
      | "Chris Paul" | {likeness: 90}  |
      | "Chris Paul" | {likeness: 100} |

  Scenario: no exist prop
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER serve YIELD $^.player.test, $$ as dst, edge as e
      """
    Then a SemanticError should be raised at runtime: `$^.player.test', not found the property `test'.
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER serve yield $^.player.test, $^ as src
      """
    Then a SemanticError should be raised at runtime: `$^.player.test', not found the property `test'.
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER serve YIELD serve.test, properties(edge) as props
      """
    Then a SemanticError should be raised at runtime: `serve.test', not found the property `test'.

  Scenario: udf call
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve WHERE $$.team.name IN ['Hawks', 'Suns'] YIELD $^ as src, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | src                                                 | dst                            |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"}) | ("Hawks" :team{name: "Hawks"}) |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"}) | ("Suns" :team{name: "Suns"})   |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst AS id |
      GO FROM  $-.id OVER serve WHERE $-.id IN ['Tony Parker', 123] YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                       |
      | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]   |
      | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}] |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._dst AS id |
      GO FROM  $-.id OVER serve WHERE $-.id IN ['Tony Parker', 123] AND 1 == 1 YIELD $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                |
      | ("Spurs" :team{name: "Spurs"})     |
      | ("Hornets" :team{name: "Hornets"}) |

  Scenario: reverse one step
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like REVERSELY YIELD $$ as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                               | e                                                           |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]   |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})     | [:like "Shaquile O'Neal"->"Tim Duncan" @0 {likeness: 80}]   |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]    |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]       |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like REVERSELY YIELD $$ as dst, edge as e, id($$), id($^)
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                               | e                                                           | id($$)              | id($^)       |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       | "Aron Baynes"       | "Tim Duncan" |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        | "Boris Diaw"        | "Tim Duncan" |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       | "Danny Green"       | "Tim Duncan" |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   | "Dejounte Murray"   | "Tim Duncan" |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] | "LaMarcus Aldridge" | "Tim Duncan" |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     | "Manu Ginobili"     | "Tim Duncan" |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]   | "Marco Belinelli"   | "Tim Duncan" |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})     | [:like "Shaquile O'Neal"->"Tim Duncan" @0 {likeness: 80}]   | "Shaquile O'Neal"   | "Tim Duncan" |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]    | "Tiago Splitter"    | "Tim Duncan" |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]       | "Tony Parker"       | "Tim Duncan" |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like REVERSELY WHERE $$.player.age < 35 YIELD distinct $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                               |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER * YIELD dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 |
      | "Manu Ginobili"     |
      | "Tony Parker"       |
      | "Spurs"             |
      | "Danny Green"       |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tony Parker"       |

  Scenario: only id n steps
    When executing query:
      """
      GO 2 STEPS FROM 'Tony Parker' OVER * YIELD distinct dst(edge)
      """
    Then the result should be, in any order, with relax comparison:
      | dst(EDGE)           |
      | "Tim Duncan"        |
      | "Tony Parker"       |
      | "Spurs"             |
      | "Trail Blazers"     |
      | "Manu Ginobili"     |
      | "Grizzlies"         |
      | "LaMarcus Aldridge" |
      | "Danny Green"       |
    When executing query:
      """
      GO 3 STEPS FROM 'Tony Parker' OVER like YIELD edge as e, $^ as src
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                            | src                                                                                                         |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
    When executing query:
      """
      GO 2 STEPS FROM 'Tony Parker' OVER * YIELD dst(edge) AS id |
      GO 2 STEPS FROM $-.id OVER * YIELD distinct src(edge) as src, dst(edge) as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | src                 | dst                 | e                                                                                    |
      | "LeBron James"      | "Ray Allen"         | [:like "LeBron James"->"Ray Allen" @0 {likeness: 100}]                               |
      | "LeBron James"      | "Cavaliers"         | [:serve "LeBron James"->"Cavaliers" @0 {end_year: 2010, start_year: 2003}]           |
      | "LeBron James"      | "Heat"              | [:serve "LeBron James"->"Heat" @0 {end_year: 2014, start_year: 2010}]                |
      | "LeBron James"      | "Lakers"            | [:serve "LeBron James"->"Lakers" @0 {end_year: 2019, start_year: 2018}]              |
      | "LeBron James"      | "Cavaliers"         | [:serve "LeBron James"->"Cavaliers" @1 {end_year: 2018, start_year: 2014}]           |
      | "Tony Parker"       | "LaMarcus Aldridge" | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]                         |
      | "Kyle Anderson"     | "Spurs"             | [:serve "Kyle Anderson"->"Spurs" @0 {end_year: 2018, start_year: 2014}]              |
      | "Kyle Anderson"     | "Grizzlies"         | [:serve "Kyle Anderson"->"Grizzlies" @0 {end_year: 2019, start_year: 2018}]          |
      | "LaMarcus Aldridge" | "Trail Blazers"     | [:serve "LaMarcus Aldridge"->"Trail Blazers" @0 {end_year: 2015, start_year: 2006}]  |
      | "LaMarcus Aldridge" | "Spurs"             | [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]          |
      | "LaMarcus Aldridge" | "Tony Parker"       | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]                         |
      | "Tony Parker"       | "Manu Ginobili"     | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | "LaMarcus Aldridge" | "Tim Duncan"        | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]                          |
      | "Danny Green"       | "Spurs"             | [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]                |
      | "Danny Green"       | "Raptors"           | [:serve "Danny Green"->"Raptors" @0 {end_year: 2019, start_year: 2018}]              |
      | "Danny Green"       | "Cavaliers"         | [:serve "Danny Green"->"Cavaliers" @0 {end_year: 2010, start_year: 2009}]            |
      | "Danny Green"       | "Tim Duncan"        | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]                                |
      | "Tony Parker"       | "Tim Duncan"        | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                                |
      | "Danny Green"       | "Marco Belinelli"   | [:like "Danny Green"->"Marco Belinelli" @0 {likeness: 83}]                           |
      | "Danny Green"       | "LeBron James"      | [:like "Danny Green"->"LeBron James" @0 {likeness: 80}]                              |
      | "Marco Belinelli"   | "Spurs"             | [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}]            |
      | "Marco Belinelli"   | "Hornets"           | [:serve "Marco Belinelli"->"Hornets" @1 {end_year: 2017, start_year: 2016}]          |
      | "Marco Belinelli"   | "Warriors"          | [:serve "Marco Belinelli"->"Warriors" @0 {end_year: 2009, start_year: 2007}]         |
      | "Tony Parker"       | "Hornets"           | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]              |
      | "Marco Belinelli"   | "Spurs"             | [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]            |
      | "Marco Belinelli"   | "Raptors"           | [:serve "Marco Belinelli"->"Raptors" @0 {end_year: 2010, start_year: 2009}]          |
      | "Marco Belinelli"   | "Kings"             | [:serve "Marco Belinelli"->"Kings" @0 {end_year: 2016, start_year: 2015}]            |
      | "Marco Belinelli"   | "Hornets"           | [:serve "Marco Belinelli"->"Hornets" @0 {end_year: 2012, start_year: 2010}]          |
      | "Marco Belinelli"   | "Hawks"             | [:serve "Marco Belinelli"->"Hawks" @0 {end_year: 2018, start_year: 2017}]            |
      | "Tony Parker"       | "Spurs"             | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]                |
      | "Marco Belinelli"   | "Bulls"             | [:serve "Marco Belinelli"->"Bulls" @0 {end_year: 2013, start_year: 2012}]            |
      | "Marco Belinelli"   | "76ers"             | [:serve "Marco Belinelli"->"76ers" @0 {end_year: 2018, start_year: 2018}]            |
      | "Marco Belinelli"   | "Tony Parker"       | [:like "Marco Belinelli"->"Tony Parker" @0 {likeness: 50}]                           |
      | "Marco Belinelli"   | "Tim Duncan"        | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]                            |
      | "Marco Belinelli"   | "Danny Green"       | [:like "Marco Belinelli"->"Danny Green" @0 {likeness: 60}]                           |
      | "Tony Parker"       | "Kyle Anderson"     | [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]     |
      | "Manu Ginobili"     | "Tony Parker"       | [:teammate "Manu Ginobili"->"Tony Parker" @0 {end_year: 2016, start_year: 2002}]     |
      | "Manu Ginobili"     | "Tim Duncan"        | [:teammate "Manu Ginobili"->"Tim Duncan" @0 {end_year: 2016, start_year: 2002}]      |
      | "Manu Ginobili"     | "Spurs"             | [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]              |
      | "Manu Ginobili"     | "Tim Duncan"        | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]                              |
      | "Tim Duncan"        | "Tony Parker"       | [:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]        |
      | "Tony Parker"       | "LaMarcus Aldridge" | [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}] |
      | "Tim Duncan"        | "Manu Ginobili"     | [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]      |
      | "Tim Duncan"        | "LaMarcus Aldridge" | [:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}]  |
      | "Tim Duncan"        | "Danny Green"       | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]        |
      | "Tim Duncan"        | "Spurs"             | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                 |
      | "Tim Duncan"        | "Tony Parker"       | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]                                |
      | "Tony Parker"       | "Manu Ginobili"     | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]     |
      | "Tim Duncan"        | "Manu Ginobili"     | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]                              |
      | "Tony Parker"       | "Tim Duncan"        | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]        |

  Scenario: reverse two steps
    When executing query:
      """
      GO 2 STEPS FROM 'Kobe Bryant' OVER like REVERSELY YIELD $$ as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                     | e                                                         |
      | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"})     | [:like "Marc Gasol"->"Paul Gasol" @0 {likeness: 99}]      |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})     | [:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]   |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"}) | [:like "Vince Carter"->"Tracy McGrady" @0 {likeness: 90}] |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})         | [:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]     |
    When executing query:
      """
      GO 2 STEPS FROM 'Kobe Bryant' OVER like REVERSELY YIELD id($$) as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | dst            | e                                                         |
      | "Marc Gasol"   | [:like "Marc Gasol"->"Paul Gasol" @0 {likeness: 99}]      |
      | "Grant Hill"   | [:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]   |
      | "Vince Carter" | [:like "Vince Carter"->"Tracy McGrady" @0 {likeness: 90}] |
      | "Yao Ming"     | [:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]     |
    When executing query:
      """
      GO FROM 'Manu Ginobili' OVER * REVERSELY YIELD src(edge) AS id |
      GO FROM $-.id OVER serve YIELD dst(edge)
      """
    Then the result should be, in any order, with relax comparison:
      | dst(EDGE) |
      | "76ers"   |
      | "Hawks"   |
      | "Spurs"   |
      | "Spurs"   |
      | "Spurs"   |
      | "Hornets" |
      | "Hornets" |
      | "Spurs"   |
      | "Spurs"   |
      | "Spurs"   |

  Scenario: reverse with pipe
    When executing query:
      """
      GO FROM 'LeBron James' OVER serve YIELD dst(edge) AS id |
      GO FROM $-.id OVER serve REVERSELY YIELD $^ as src, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | src                                    | dst                                                               |
      | ("Heat" :team{name: "Heat"})           | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"}) |
      | ("Heat" :team{name: "Heat"})           | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})             |
      | ("Heat" :team{name: "Heat"})           | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
      | ("Heat" :team{name: "Heat"})           | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                 |
      | ("Heat" :team{name: "Heat"})           | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})     |
      | ("Heat" :team{name: "Heat"})           | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})             |
      | ("Lakers" :team{name: "Lakers"})       | ("Dwight Howard" :player{age: 33, name: "Dwight Howard"})         |
      | ("Lakers" :team{name: "Lakers"})       | ("JaVale McGee" :player{age: 31, name: "JaVale McGee"})           |
      | ("Lakers" :team{name: "Lakers"})       | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})             |
      | ("Lakers" :team{name: "Lakers"})       | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
      | ("Lakers" :team{name: "Lakers"})       | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})               |
      | ("Lakers" :team{name: "Lakers"})       | ("Rajon Rondo" :player{age: 33, name: "Rajon Rondo"})             |
      | ("Lakers" :team{name: "Lakers"})       | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})     |
      | ("Lakers" :team{name: "Lakers"})       | ("Steve Nash" :player{age: 45, name: "Steve Nash"})               |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})             |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})             |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("Kyrie Irving" :player{age: 26, name: "Kyrie Irving"})           |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("Kyrie Irving" :player{age: 26, name: "Kyrie Irving"})           |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})     |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})     |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
      | ("Cavaliers" :team{name: "Cavaliers"}) | ("LeBron James" :player{age: 34, name: "LeBron James"})           |
    When executing query:
      """
      GO FROM 'LeBron James' OVER serve YIELD dst(edge) AS id |
      GO FROM $-.id OVER serve REVERSELY WHERE $$.player.name != 'LeBron James'
      YIELD distinct edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                             |
      | [:serve "Amar'e Stoudemire"->"Heat" @0 {end_year: 2016, start_year: 2015}]    |
      | [:serve "Dwyane Wade"->"Heat" @0 {end_year: 2016, start_year: 2003}]          |
      | [:serve "Shaquile O'Neal"->"Cavaliers" @0 {end_year: 2010, start_year: 2009}] |
      | [:serve "Ray Allen"->"Heat" @0 {end_year: 2014, start_year: 2012}]            |
      | [:serve "Shaquile O'Neal"->"Heat" @0 {end_year: 2008, start_year: 2004}]      |
      | [:serve "Dwyane Wade"->"Heat" @1 {end_year: 2019, start_year: 2018}]          |
      | [:serve "Dwight Howard"->"Lakers" @0 {end_year: 2013, start_year: 2012}]      |
      | [:serve "JaVale McGee"->"Lakers" @0 {end_year: 2019, start_year: 2018}]       |
      | [:serve "Kobe Bryant"->"Lakers" @0 {end_year: 2016, start_year: 1996}]        |
      | [:serve "Kyrie Irving"->"Cavaliers" @0 {end_year: 2017, start_year: 2011}]    |
      | [:serve "Paul Gasol"->"Lakers" @0 {end_year: 2014, start_year: 2008}]         |
      | [:serve "Rajon Rondo"->"Lakers" @0 {end_year: 2019, start_year: 2018}]        |
      | [:serve "Shaquile O'Neal"->"Lakers" @0 {end_year: 2004, start_year: 1996}]    |
      | [:serve "Steve Nash"->"Lakers" @0 {end_year: 2015, start_year: 2012}]         |
      | [:serve "Danny Green"->"Cavaliers" @0 {end_year: 2010, start_year: 2009}]     |
      | [:serve "Dwyane Wade"->"Cavaliers" @0 {end_year: 2018, start_year: 2017}]     |
    When executing query:
      """
      GO FROM 'Manu Ginobili' OVER like REVERSELY YIELD src(edge) AS id |
      GO FROM $-.id OVER serve YIELD distinct dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst       |
      | "Spurs"   |
      | "Hornets" |
      | "Hawks"   |
      | "76ers"   |

  Scenario: bidirect
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER serve bidirect YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                    |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}] |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like bidirect YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                           |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]   |
      | [:like "Shaquile O'Neal"->"Tim Duncan" @0 {likeness: 80}]   |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]    |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]       |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]     |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]       |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER serve, like bidirect YIELD distinct dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst             |
      | "Tim Duncan"    |
      | "Tony Parker"   |
      | "Manu Ginobili" |
      | "Spurs"         |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like bidirect YIELD $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                               |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})     |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like bidirect WHERE like.likeness > 90 YIELD properties(edge) as props, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | props          | e                                                         |
      | {likeness: 99} | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}] |
      | {likeness: 95} | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]     |
      | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]   |
      | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]     |

  Scenario: bidirect_over_all
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER * bidirect YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                   |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]                               |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]                                |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]                               |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]                           |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]                         |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]                             |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]                           |
      | [:like "Shaquile O'Neal"->"Tim Duncan" @0 {likeness: 80}]                           |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]                            |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                               |
      | [:teammate "Manu Ginobili"->"Tim Duncan" @0 {end_year: 2016, start_year: 2002}]     |
      | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]       |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]                               |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                |
      | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]       |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}] |
      | [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]     |
      | [:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]       |

  Scenario: duplicate column name
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER serve YIELD edge as e, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                    | e                                                                    |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}] | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}] |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD dst(edge) AS id, like.likeness AS id |
      GO FROM $-.id OVER serve YIELD edge as e
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like, serve YIELD serve.start_year AS year, serve.end_year AS year, dst(edge) AS id |
      GO FROM $-.id OVER * YIELD $$ as dst
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `year'
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER * YIELD dst(edge) as id;
      | GO FROM $-.id OVER serve
      """
    Then a SyntaxError should be raised at runtime: syntax error near `| GO FRO'

  Scenario: invalid condition in where
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like where like.likeness YIELD edge as e
      """
    Then a SemanticError should be raised at runtime: `like.likeness', expected boolean, but was `INT'

  Scenario: contain
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve WHERE $$.team.name CONTAINS "Haw" YIELD $^ as src, $$ as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | src                                                 | dst                            | e                                                                    |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"}) | ("Hawks" :team{name: "Hawks"}) | [:serve "Boris Diaw"->"Hawks" @0 {end_year: 2005, start_year: 2003}] |
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve WHERE (string)serve.start_year CONTAINS "05" YIELD edge as e, properties(edge) as props
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                   | props                              |
      | [:serve "Boris Diaw"->"Suns" @0 {end_year: 2008, start_year: 2005}] | {end_year: 2008, start_year: 2005} |
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve WHERE $^.player.name CONTAINS "Boris" YIELD $$ as dst, dst(edge) as dstvid, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                | dstvid    | e                                                                      |
      | ("Hawks" :team{name: "Hawks"})     | "Hawks"   | [:serve "Boris Diaw"->"Hawks" @0 {end_year: 2005, start_year: 2003}]   |
      | ("Hornets" :team{name: "Hornets"}) | "Hornets" | [:serve "Boris Diaw"->"Hornets" @0 {end_year: 2012, start_year: 2008}] |
      | ("Jazz" :team{name: "Jazz"})       | "Jazz"    | [:serve "Boris Diaw"->"Jazz" @0 {end_year: 2017, start_year: 2016}]    |
      | ("Spurs" :team{name: "Spurs"})     | "Spurs"   | [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]   |
      | ("Suns" :team{name: "Suns"})       | "Suns"    | [:serve "Boris Diaw"->"Suns" @0 {end_year: 2008, start_year: 2005}]    |
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve WHERE !($^.player.name CONTAINS "Boris") YIELD $$ as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | dst | e |
    When executing query:
      """
      GO FROM 'Boris Diaw' OVER serve WHERE "Leo" CONTAINS "Boris" YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e |

  Scenario: [0] intermediate data
    When executing query:
      """
      GO 0 TO 0 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "LaMarcus Aldridge" |
      | "Tim Duncan"        |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT dst(edge) as dst, edge as e, $$.player.name, $^.player.age, like.likeness
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 | e                                                            | $$.player.name      | $^.player.age | like.likeness |
      | "LaMarcus Aldridge" | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] | "LaMarcus Aldridge" | 36            | 90            |
      | "Manu Ginobili"     | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     | "Manu Ginobili"     | 36            | 95            |
      | "Tim Duncan"        | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        | "Tim Duncan"        | 36            | 95            |
      | "Tim Duncan"        | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      | "Tim Duncan"        | 41            | 90            |
      | "Tim Duncan"        | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]  | "Tim Duncan"        | 33            | 75            |
      | "Tony Parker"       | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}] | "Tony Parker"       | 33            | 75            |
      | "Manu Ginobili"     | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      | "Manu Ginobili"     | 42            | 95            |
      | "Tony Parker"       | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        | "Tony Parker"       | 42            | 95            |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT $$ as dst, $^ as src
      """
    Then the result should be, in any order, with relax comparison:
      | dst                                                                                                         | src                                                                                                         |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT id($$) as dst, $^ as src
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 | src                                                                                                         |
      | "LaMarcus Aldridge" | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | "Manu Ginobili"     | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | "Tim Duncan"        | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | "Tim Duncan"        | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "Tim Duncan"        | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | "Tony Parker"       | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | "Manu Ginobili"     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | "Tony Parker"       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT like._dst, like.likeness, $$.player.name, dst(edge) as dst, src(edge) as src
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst           | like.likeness | $$.player.name      | dst                 | src                 |
      | "LaMarcus Aldridge" | 90            | "LaMarcus Aldridge" | "LaMarcus Aldridge" | "Tony Parker"       |
      | "Manu Ginobili"     | 95            | "Manu Ginobili"     | "Manu Ginobili"     | "Tony Parker"       |
      | "Tim Duncan"        | 95            | "Tim Duncan"        | "Tim Duncan"        | "Tony Parker"       |
      | "Tim Duncan"        | 90            | "Tim Duncan"        | "Tim Duncan"        | "Manu Ginobili"     |
      | "Tim Duncan"        | 75            | "Tim Duncan"        | "Tim Duncan"        | "LaMarcus Aldridge" |
      | "Tony Parker"       | 75            | "Tony Parker"       | "Tony Parker"       | "LaMarcus Aldridge" |
      | "Manu Ginobili"     | 95            | "Manu Ginobili"     | "Manu Ginobili"     | "Tim Duncan"        |
      | "Tony Parker"       | 95            | "Tony Parker"       | "Tony Parker"       | "Tim Duncan"        |
    When executing query:
      """
      GO 0 TO 3 STEPS FROM 'Tim Duncan' OVER serve YIELD edge as e, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                    | dst                            |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}] | ("Spurs" :team{name: "Spurs"}) |
    When executing query:
      """
      GO 2 TO 3 STEPS FROM 'Tim Duncan' OVER serve YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT src(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 |
      | "Tim Duncan"        |
      | "LaMarcus Aldridge" |
      | "Marco Belinelli"   |
      | "Boris Diaw"        |
      | "Dejounte Murray"   |
      | "Tony Parker"       |
      | "Manu Ginobili"     |
      | "Danny Green"       |
      | "Aron Baynes"       |
      | "Tiago Splitter"    |
      | "Shaquile O'Neal"   |
      | "Rudy Gay"          |
      | "Damian Lillard"    |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                               |
      | [:like "Boris Diaw"->"Tony Parker" @0 {likeness: 80}]           |
      | [:like "Dejounte Murray"->"Tony Parker" @0 {likeness: 99}]      |
      | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]    |
      | [:like "Marco Belinelli"->"Tony Parker" @0 {likeness: 50}]      |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]           |
      | [:like "Damian Lillard"->"LaMarcus Aldridge" @0 {likeness: 80}] |
      | [:like "Rudy Gay"->"LaMarcus Aldridge" @0 {likeness: 70}]       |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]    |
      | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]           |
      | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]            |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]           |
      | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]       |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]     |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]         |
      | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]       |
      | [:like "Shaquile O'Neal"->"Tim Duncan" @0 {likeness: 80}]       |
      | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]        |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]           |
      | [:like "Danny Green"->"Marco Belinelli" @0 {likeness: 83}]      |
      | [:like "Dejounte Murray"->"Marco Belinelli" @0 {likeness: 99}]  |
    When executing query:
      """
      GO 2 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT $$ as name
      """
    Then the result should be, in any order, with relax comparison:
      | name                                                              |
      | ("Damian Lillard" :player{age: 28, name: "Damian Lillard"})       |
      | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                   |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
      | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})             |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})               |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})     |
      | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})     |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})     |
      | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})       |
    When executing query:
      """
      GO 1 TO 3 STEPS FROM 'Spurs' OVER serve REVERSELY YIELD type(edge) as type, rank(edge) as rank,  $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | type    | rank | dst                                                                                                         |
      | "serve" | 0    | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | "serve" | 0    | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | "serve" | 0    | ("Cory Joseph" :player{age: 27, name: "Cory Joseph"})                                                       |
      | "serve" | 0    | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | "serve" | 0    | ("David West" :player{age: 38, name: "David West"})                                                         |
      | "serve" | 0    | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | "serve" | 0    | ("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})                                             |
      | "serve" | 0    | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | "serve" | 0    | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | "serve" | 0    | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "serve" | 0    | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | "serve" | 0    | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | "serve" | 0    | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | "serve" | 0    | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | "serve" | 0    | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | "serve" | 0    | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | "serve" | 0    | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | "serve" | 1    | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
    When executing query:
      """
      GO 0 TO 3 STEPS FROM 'Spurs' OVER serve REVERSELY YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                           |
      | [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}]       |
      | [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]        |
      | [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}]       |
      | [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]       |
      | [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}]        |
      | [:serve "Dejounte Murray"->"Spurs" @0 {end_year: 2019, start_year: 2016}]   |
      | [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}]  |
      | [:serve "Kyle Anderson"->"Spurs" @0 {end_year: 2018, start_year: 2014}]     |
      | [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}] |
      | [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]     |
      | [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]   |
      | [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]        |
      | [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}]          |
      | [:serve "Tiago Splitter"->"Spurs" @0 {end_year: 2015, start_year: 2010}]    |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]        |
      | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]       |
      | [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}]     |
      | [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}]   |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like BIDIRECT YIELD DISTINCT src(edge) as src, dst(edge) as dst
      """
    Then the result should be, in any order, with relax comparison:
      | src                 | dst                 |
      | "Boris Diaw"        | "Tony Parker"       |
      | "Dejounte Murray"   | "Tony Parker"       |
      | "LaMarcus Aldridge" | "Tony Parker"       |
      | "Marco Belinelli"   | "Tony Parker"       |
      | "Tim Duncan"        | "Tony Parker"       |
      | "Tony Parker"       | "LaMarcus Aldridge" |
      | "Tony Parker"       | "Manu Ginobili"     |
      | "Tony Parker"       | "Tim Duncan"        |
      | "Danny Green"       | "Marco Belinelli"   |
      | "Dejounte Murray"   | "Marco Belinelli"   |
      | "Marco Belinelli"   | "Danny Green"       |
      | "Marco Belinelli"   | "Tim Duncan"        |
      | "Dejounte Murray"   | "Russell Westbrook" |
      | "Aron Baynes"       | "Tim Duncan"        |
      | "Boris Diaw"        | "Tim Duncan"        |
      | "Danny Green"       | "Tim Duncan"        |
      | "Dejounte Murray"   | "Tim Duncan"        |
      | "LaMarcus Aldridge" | "Tim Duncan"        |
      | "Manu Ginobili"     | "Tim Duncan"        |
      | "Dejounte Murray"   | "LeBron James"      |
      | "Shaquile O'Neal"   | "Tim Duncan"        |
      | "Tiago Splitter"    | "Tim Duncan"        |
      | "Dejounte Murray"   | "Kyle Anderson"     |
      | "Tim Duncan"        | "Manu Ginobili"     |
      | "Dejounte Murray"   | "Kevin Durant"      |
      | "Damian Lillard"    | "LaMarcus Aldridge" |
      | "Rudy Gay"          | "LaMarcus Aldridge" |
      | "Dejounte Murray"   | "James Harden"      |
      | "Tiago Splitter"    | "Manu Ginobili"     |
      | "Dejounte Murray"   | "Danny Green"       |
      | "Dejounte Murray"   | "Manu Ginobili"     |
      | "Dejounte Murray"   | "Chris Paul"        |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * YIELD serve._dst, like._dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | serve._dst | like._dst           | e                                                                              |
      | EMPTY      | "James Harden"      | [:like "Russell Westbrook"->"James Harden" @0 {likeness: 90}]                  |
      | EMPTY      | "Paul George"       | [:like "Russell Westbrook"->"Paul George" @0 {likeness: 90}]                   |
      | "Thunders" | EMPTY               | [:serve "Russell Westbrook"->"Thunders" @0 {end_year: 2019, start_year: 2008}] |
      | EMPTY      | "Russell Westbrook" | [:like "James Harden"->"Russell Westbrook" @0 {likeness: 80}]                  |
      | "Rockets"  | EMPTY               | [:serve "James Harden"->"Rockets" @0 {end_year: 2019, start_year: 2012}]       |
      | "Thunders" | EMPTY               | [:serve "James Harden"->"Thunders" @0 {end_year: 2012, start_year: 2009}]      |
      | EMPTY      | "Russell Westbrook" | [:like "Paul George"->"Russell Westbrook" @0 {likeness: 95}]                   |
      | "Pacers"   | EMPTY               | [:serve "Paul George"->"Pacers" @0 {end_year: 2017, start_year: 2010}]         |
      | "Thunders" | EMPTY               | [:serve "Paul George"->"Thunders" @0 {end_year: 2019, start_year: 2017}]       |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM 'Russell Westbrook' OVER * YIELD edge as e, $$ as dst, $^ as src
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                              | dst                                                               | src                                                               |
      | [:like "Russell Westbrook"->"James Harden" @0 {likeness: 90}]                  | ("James Harden" :player{age: 29, name: "James Harden"})           | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) |
      | [:like "Russell Westbrook"->"Paul George" @0 {likeness: 90}]                   | ("Paul George" :player{age: 28, name: "Paul George"})             | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) |
      | [:serve "Russell Westbrook"->"Thunders" @0 {end_year: 2019, start_year: 2008}] | ("Thunders" :team{name: "Thunders"})                              | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) |
      | [:like "James Harden"->"Russell Westbrook" @0 {likeness: 80}]                  | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) | ("James Harden" :player{age: 29, name: "James Harden"})           |
      | [:serve "James Harden"->"Rockets" @0 {end_year: 2019, start_year: 2012}]       | ("Rockets" :team{name: "Rockets"})                                | ("James Harden" :player{age: 29, name: "James Harden"})           |
      | [:serve "James Harden"->"Thunders" @0 {end_year: 2012, start_year: 2009}]      | ("Thunders" :team{name: "Thunders"})                              | ("James Harden" :player{age: 29, name: "James Harden"})           |
      | [:like "Paul George"->"Russell Westbrook" @0 {likeness: 95}]                   | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) | ("Paul George" :player{age: 28, name: "Paul George"})             |
      | [:serve "Paul George"->"Pacers" @0 {end_year: 2017, start_year: 2010}]         | ("Pacers" :team{name: "Pacers"})                                  | ("Paul George" :player{age: 28, name: "Paul George"})             |
      | [:serve "Paul George"->"Thunders" @0 {end_year: 2019, start_year: 2017}]       | ("Thunders" :team{name: "Thunders"})                              | ("Paul George" :player{age: 28, name: "Paul George"})             |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * YIELD dst(edge) as dst, serve.start_year, like.likeness, $$.player.name
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 | serve.start_year | like.likeness | $$.player.name      |
      | "James Harden"      | EMPTY            | 90            | "James Harden"      |
      | "Paul George"       | EMPTY            | 90            | "Paul George"       |
      | "Thunders"          | 2008             | EMPTY         | EMPTY               |
      | "Russell Westbrook" | EMPTY            | 80            | "Russell Westbrook" |
      | "Rockets"           | 2012             | EMPTY         | EMPTY               |
      | "Thunders"          | 2009             | EMPTY         | EMPTY               |
      | "Russell Westbrook" | EMPTY            | 95            | "Russell Westbrook" |
      | "Pacers"            | 2010             | EMPTY         | EMPTY               |
      | "Thunders"          | 2017             | EMPTY         | EMPTY               |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * REVERSELY YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                |
      | [:like "Dejounte Murray"->"Russell Westbrook" @0 {likeness: 99}] |
      | [:like "James Harden"->"Russell Westbrook" @0 {likeness: 80}]    |
      | [:like "Paul George"->"Russell Westbrook" @0 {likeness: 95}]     |
      | [:like "Dejounte Murray"->"James Harden" @0 {likeness: 99}]      |
      | [:like "Luka Doncic"->"James Harden" @0 {likeness: 80}]          |
      | [:like "Russell Westbrook"->"James Harden" @0 {likeness: 90}]    |
      | [:like "Russell Westbrook"->"Paul George" @0 {likeness: 90}]     |
    When executing query:
      """
      GO 0 TO 2 STEPS FROM 'Russell Westbrook' OVER * REVERSELY YIELD $^.player.name, $^ as src, type(edge) as type
      """
    Then the result should be, in any order, with relax comparison:
      | $^.player.name      | src                                                               | type   |
      | "Russell Westbrook" | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) | "like" |
      | "Russell Westbrook" | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) | "like" |
      | "Russell Westbrook" | ("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"}) | "like" |
      | "James Harden"      | ("James Harden" :player{age: 29, name: "James Harden"})           | "like" |
      | "James Harden"      | ("James Harden" :player{age: 29, name: "James Harden"})           | "like" |
      | "James Harden"      | ("James Harden" :player{age: 29, name: "James Harden"})           | "like" |
      | "Paul George"       | ("Paul George" :player{age: 28, name: "Paul George"})             | "like" |
    When executing query:
      """
      GO 1 to 3 steps FROM "Tim Duncan" OVER like YIELD edge as e, properties(edge) as props, concat(src(edge), " like ", dst(edge), " @ ", properties($$).name, " # ", properties($^).age)  as result
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                            | props          | result                                                        |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      | {likeness: 95} | "Tim Duncan like Manu Ginobili @ Manu Ginobili # 42"          |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        | {likeness: 95} | "Tim Duncan like Tony Parker @ Tony Parker # 42"              |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      | {likeness: 90} | "Manu Ginobili like Tim Duncan @ Tim Duncan # 41"             |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] | {likeness: 90} | "Tony Parker like LaMarcus Aldridge @ LaMarcus Aldridge # 36" |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     | {likeness: 95} | "Tony Parker like Manu Ginobili @ Manu Ginobili # 36"         |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        | {likeness: 95} | "Tony Parker like Tim Duncan @ Tim Duncan # 36"               |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]  | {likeness: 75} | "LaMarcus Aldridge like Tim Duncan @ Tim Duncan # 33"         |
      | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}] | {likeness: 75} | "LaMarcus Aldridge like Tony Parker @ Tony Parker # 33"       |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      | {likeness: 95} | "Tim Duncan like Manu Ginobili @ Manu Ginobili # 42"          |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        | {likeness: 95} | "Tim Duncan like Tony Parker @ Tony Parker # 42"              |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      | {likeness: 90} | "Manu Ginobili like Tim Duncan @ Tim Duncan # 41"             |

  Scenario: error message
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER serve YIELD $$.player.name as name, edge as e, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | name  | e                                                                    | dst                            |
      | EMPTY | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}] | ("Spurs" :team{name: "Spurs"}) |

  Scenario: zero step
    When executing query:
      """
      GO 0 STEPS FROM 'Tim Duncan' OVER serve BIDIRECT YIELD $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst |
    When executing query:
      """
      GO 0 STEPS FROM 'Tim Duncan' OVER serve YIELD edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | e |
    When executing query:
      """
      GO 0 STEPS FROM 'Tim Duncan' OVER like YIELD dst(edge) as id |
      GO FROM $-.id OVER serve YIELD dst(edge) as id
      """
    Then the result should be, in any order, with relax comparison:
      | id |

  Scenario: go cover input
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD src(edge) as src, dst(edge) as dst |
      GO FROM $-.src OVER like YIELD $-.src as src, like._dst as dst, $^ as a, $$ as b
      """
    Then the result should be, in any order, with relax comparison:
      | src          | dst             | a                                                                                                           | b                                                         |
      | "Tim Duncan" | "Manu Ginobili" | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
      | "Tim Duncan" | "Manu Ginobili" | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"}) |
      | "Tim Duncan" | "Tony Parker"   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
      | "Tim Duncan" | "Tony Parker"   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"})     |
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' OVER like YIELD src(edge) as src, dst(edge) as dst;
      GO FROM $a.src OVER like YIELD $a.src as src, like._dst as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | src          | dst             | e                                                       |
      | "Tim Duncan" | "Manu Ginobili" | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | "Tim Duncan" | "Manu Ginobili" | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}] |
      | "Tim Duncan" | "Tony Parker"   | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
      | "Tim Duncan" | "Tony Parker"   | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]   |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst |
      GO 1 TO 2 STEPS FROM $-.src OVER like YIELD $-.src as src, like._dst as dst, $^ as snode, $$ as dnode
      """
    Then the result should be, in any order, with relax comparison:
      | src          | dst                 | snode                                                                                                       | dnode                                                                                                       |
      | "Tim Duncan" | "Manu Ginobili"     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "Tim Duncan" | "Manu Ginobili"     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "Tim Duncan" | "Tony Parker"       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | "Tim Duncan" | "Tony Parker"       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | "Tim Duncan" | "Tim Duncan"        | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | "Tim Duncan" | "Tim Duncan"        | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | "Tim Duncan" | "LaMarcus Aldridge" | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | "Tim Duncan" | "LaMarcus Aldridge" | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | "Tim Duncan" | "Manu Ginobili"     | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "Tim Duncan" | "Manu Ginobili"     | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "Tim Duncan" | "Tim Duncan"        | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | "Tim Duncan" | "Tim Duncan"        | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst |
      GO 1 TO 2 STEPS FROM $-.src OVER like YIELD $-.src as src, $-.dst, like._dst as dst, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | src          | $-.dst          | dst                 | e                                                            |
      | "Tim Duncan" | "Manu Ginobili" | "Manu Ginobili"     | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | "Tim Duncan" | "Tony Parker"   | "Manu Ginobili"     | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | "Tim Duncan" | "Manu Ginobili" | "Tony Parker"       | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | "Tim Duncan" | "Tony Parker"   | "Tony Parker"       | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | "Tim Duncan" | "Manu Ginobili" | "Tim Duncan"        | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Tim Duncan" | "Tony Parker"   | "Tim Duncan"        | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Tim Duncan" | "Manu Ginobili" | "LaMarcus Aldridge" | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | "Tim Duncan" | "Tony Parker"   | "LaMarcus Aldridge" | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}] |
      | "Tim Duncan" | "Manu Ginobili" | "Manu Ginobili"     | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | "Tim Duncan" | "Tony Parker"   | "Manu Ginobili"     | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]     |
      | "Tim Duncan" | "Manu Ginobili" | "Tim Duncan"        | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
      | "Tim Duncan" | "Tony Parker"   | "Tim Duncan"        | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]        |
    When executing query:
      """
      GO FROM 'Danny Green' OVER like YIELD src(edge) AS src, dst(edge) AS dst |
      GO FROM $-.dst OVER teammate YIELD $-.src AS src, $-.dst, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | src           | $-.dst       | dst                                                               |
      | "Danny Green" | "Tim Duncan" | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | "Danny Green" | "Tim Duncan" | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | "Danny Green" | "Tim Duncan" | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | "Danny Green" | "Tim Duncan" | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |
    When executing query:
      """
      $a = GO FROM 'Danny Green' OVER like YIELD like._src AS src, like._dst AS dst;
      GO FROM $a.dst OVER teammate YIELD $a.src AS src, $a.dst, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | src           | $a.dst       | dst                                                               |
      | "Danny Green" | "Tim Duncan" | ("Danny Green" :player{age: 31, name: "Danny Green"})             |
      | "Danny Green" | "Tim Duncan" | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"}) |
      | "Danny Green" | "Tim Duncan" | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})         |
      | "Danny Green" | "Tim Duncan" | ("Tony Parker" :player{age: 36, name: "Tony Parker"})             |

  Scenario: backtrack overlap
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like YIELD like._src as src, dst(edge) as dst |
      GO 2 STEPS FROM $-.src OVER like YIELD $-.src, $-.dst, properties(edge) as props, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | $-.src        | $-.dst              | props          | e                                                            |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 90} | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 90} | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 90} | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]  |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]  |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]  |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}] |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}] |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}] |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
    When executing query:
      """
      $a = GO FROM 'Tony Parker' OVER like YIELD like._src as src, like._dst as dst;
      GO 2 STEPS FROM $a.src OVER like YIELD $a.src, $a.dst, properties(edge) as props, edge as e
      """
    Then the result should be, in any order, with relax comparison:
      | $a.src        | $a.dst              | props          | e                                                            |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 90} | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 90} | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 90} | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]      |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]  |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]  |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]  |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}] |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}] |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 75} | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}] |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 95} | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]      |
      | "Tony Parker" | "LaMarcus Aldridge" | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | "Tony Parker" | "Manu Ginobili"     | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |
      | "Tony Parker" | "Tim Duncan"        | {likeness: 95} | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]        |

  Scenario: Go and Limit
    When executing query:
      """
      $a = GO FROM 'Tony Parker' OVER like YIELD src(edge) as src, dst(edge) as dst;
      GO 2 STEPS FROM $a.src OVER like YIELD $a.src as src, $a.dst, src(edge) as like_src, like._dst
      | ORDER BY $-.src,$-.like_src | OFFSET 1 LIMIT 2
      """
    Then the result should be, in any order, with relax comparison:
      | src           | $a.dst          | like_src            | like._dst    |
      | "Tony Parker" | "Manu Ginobili" | "LaMarcus Aldridge" | "Tim Duncan" |
      | "Tony Parker" | "Tim Duncan"    | "LaMarcus Aldridge" | "Tim Duncan" |
    When executing query:
      """
      $a = GO FROM 'Tony Parker' OVER like YIELD like._src as src, like._dst as dst;
      GO 2 STEPS FROM $a.src OVER like YIELD $a.src as src, $$ as dst, src(edge) as like_src, edge as e
      | ORDER BY $-.src,$-.like_src | LIMIT 2 OFFSET 1
      """
    Then the result should be, in any order, with relax comparison:
      | src           | dst                                                                                                         | like_src            | e                                                           |
      | "Tony Parker" | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | "LaMarcus Aldridge" | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] |
      | "Tony Parker" | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | "LaMarcus Aldridge" | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] |

  Scenario: GroupBy and Count
    When executing query:
      """
      GO 1 TO 2 STEPS FROM "Tim Duncan" OVER like WHERE like._dst != "YAO MING" YIELD dst(edge) AS vid
      | GROUP BY $-.vid YIELD 1 AS id
      | GROUP BY $-.id YIELD COUNT($-.id);
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT($-.id) |
      | 4            |
    When executing query:
      """
      GO 1 TO 2 STEPS FROM "Tim Duncan" OVER * WHERE like._dst != "YAO MING" YIELD dst(edge) AS vid
      | GROUP BY $-.vid YIELD 1 AS id
      | GROUP BY $-.id YIELD COUNT($-.id);
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT($-.id) |
      | 13           |

  Scenario: Bugfix filter not pushdown
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like WHERE like._dst == "Tony Parker" YIELD edge as e | limit 10;
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                     |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}] |

  Scenario: Step over end
    When executing query:
      """
      GO 2 STEPS FROM "Tim Duncan" OVER serve YIELD edge as e
      """
    Then the result should be, in any order:
      | e |
    When executing query:
      """
      GO 10 STEPS FROM "Tim Duncan" OVER serve YIELD $$ as dst
      """
    Then the result should be, in any order:
      | dst |
    When executing query:
      """
      GO 10000000000000 STEPS FROM "Tim Duncan" OVER serve YIELD dst(edge) as dst
      """
    Then the result should be, in any order:
      | dst |
    When executing query:
      """
      GO 1 TO 10 STEPS FROM "Tim Duncan" OVER serve YIELD $$ as dst
      """
    Then the result should be, in any order:
      | dst                            |
      | ("Spurs" :team{name: "Spurs"}) |
    When executing query:
      """
      GO 2 TO 10 STEPS FROM "Tim Duncan" OVER serve YIELD edge as e, $$ as dst
      """
    Then the result should be, in any order:
      | e | dst |
    When executing query:
      """
      GO 1000000000 TO 1000000002 STEPS FROM "Tim Duncan" OVER serve YIELD dst(edge) as dst, src(edge) as src
      """
    Then the result should be, in any order:
      | dst | src |

  @skip
  Scenario: go step limit
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like LIMIT [10,10];
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like LIMIT ["10"];
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like LIMIT [a];
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like LIMIT [1];
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |
    When executing query:
      """
      GO 3 STEPS FROM "Tim Duncan" OVER like LIMIT [1, 2, 2];
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |

  @skip
  Scenario: go step filter & step limit
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like WHERE [like._dst == "Tony Parker"]  LIMIT [1];
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |
    When executing query:
      """
      GO 3 STEPS FROM "Tim Duncan" OVER like WHERE [like._dst == "Tony Parker", $$.player.age>20, $$.player.age>22] LIMIT [1, 2, 2];
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |

  @skip
  Scenario: go step sample
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like SAMPLE [10,10];
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like SAMPLE ["10"];
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like SAMPLE [a];
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like SAMPLE [1];
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |
    When executing query:
      """
      GO 3 STEPS FROM "Tim Duncan" OVER like SAMPLE [1, 3, 2];
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |

  @skip
  Scenario: go step filter & step sample
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like WHERE [like._dst == "Tony Parker"]  SAMPLE [1];
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |
    When executing query:
      """
      GO 3 STEPS FROM "Tim Duncan" OVER like WHERE [like._dst == "Tony Parker", $$.player.age>20, $$.player.age>22] SAMPLE [1, 2, 2];
      """
    Then the result should be, in any order, with relax comparison:
      | like._dst |

  Scenario: with no existed tag
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like YIELD $$.player.name, $^.team.name, $$ as dst
      """
    Then the result should be, in any order, with relax comparison:
      | $$.player.name      | $^.team.name | dst                                                                                                         |
      | "LaMarcus Aldridge" | EMPTY        | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | "Manu Ginobili"     | EMPTY        | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | "Tim Duncan"        | EMPTY        | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

  Scenario: go from vertices looked up by index
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name |
      GO 1 TO 2 STEPS FROM $-.name OVER like REVERSELY YIELD like._dst AS dst, $$.player.name AS name, edge as e, $$ as dstnode, $^ as srcnode
      """
    Then the result should be, in any order, with relax comparison:
      | dst                  | name                 | e                                                             | dstnode                                                             | srcnode                                                   |
      | "Jason Kidd"         | "Jason Kidd"         | [:like "Jason Kidd"->"Dirk Nowitzki" @0 {likeness: 85}]       | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                 | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"}) |
      | "Luka Doncic"        | "Luka Doncic"        | [:like "Luka Doncic"->"Dirk Nowitzki" @0 {likeness: 90}]      | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"})               | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"}) |
      | "Steve Nash"         | "Steve Nash"         | [:like "Steve Nash"->"Dirk Nowitzki" @0 {likeness: 88}]       | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                 | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"}) |
      | "Paul Gasol"         | "Paul Gasol"         | [:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}]         | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                 | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})     |
      | "Tracy McGrady"      | "Tracy McGrady"      | [:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}]      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})           | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})     |
      | "Kristaps Porzingis" | "Kristaps Porzingis" | [:like "Kristaps Porzingis"->"Luka Doncic" @0 {likeness: 90}] | ("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"}) | ("Luka Doncic" :player{age: 20, name: "Luka Doncic"})     |
      | "Dirk Nowitzki"      | "Dirk Nowitzki"      | [:like "Dirk Nowitzki"->"Jason Kidd" @0 {likeness: 80}]       | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})           | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})       |
      | "Steve Nash"         | "Steve Nash"         | [:like "Steve Nash"->"Jason Kidd" @0 {likeness: 85}]          | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                 | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})       |
      | "Vince Carter"       | "Vince Carter"       | [:like "Vince Carter"->"Jason Kidd" @0 {likeness: 70}]        | ("Vince Carter" :player{age: 42, name: "Vince Carter"})             | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})       |
      | "Marc Gasol"         | "Marc Gasol"         | [:like "Marc Gasol"->"Paul Gasol" @0 {likeness: 99}]          | ("Marc Gasol" :player{age: 34, name: "Marc Gasol"})                 | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})       |
      | "Amar'e Stoudemire"  | "Amar'e Stoudemire"  | [:like "Amar'e Stoudemire"->"Steve Nash" @0 {likeness: 90}]   | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})   | ("Steve Nash" :player{age: 45, name: "Steve Nash"})       |
      | "Dirk Nowitzki"      | "Dirk Nowitzki"      | [:like "Dirk Nowitzki"->"Steve Nash" @0 {likeness: 80}]       | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})           | ("Steve Nash" :player{age: 45, name: "Steve Nash"})       |
      | "Jason Kidd"         | "Jason Kidd"         | [:like "Jason Kidd"->"Steve Nash" @0 {likeness: 90}]          | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                 | ("Steve Nash" :player{age: 45, name: "Steve Nash"})       |
      | "Grant Hill"         | "Grant Hill"         | [:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]       | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                 | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"}) |
      | "Vince Carter"       | "Vince Carter"       | [:like "Vince Carter"->"Tracy McGrady" @0 {likeness: 90}]     | ("Vince Carter" :player{age: 42, name: "Vince Carter"})             | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"}) |
      | "Yao Ming"           | "Yao Ming"           | [:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]         | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"}) |

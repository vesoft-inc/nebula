# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Starts With Expression

  Background:
    Given a graph with space named "nba"

  Scenario: yield starts with
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "app") |
      | true                        |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "a") |
      | true                      |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'A'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "A") |
      | false                     |
    When executing query:
      """
      YIELD 'apple' STARTS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" STARTS WITH "b") |
      | false                     |
    When executing query:
      """
      YIELD '123' STARTS WITH '1'
      """
    Then the result should be, in any order:
      | ("123" STARTS WITH "1") |
      | true                    |
    When executing query:
      """
      YIELD 123 STARTS WITH 1
      """
    Then a SemanticError should be raised at runtime: `1', expected String, but wat INT

  Scenario: yield not starts with
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "app") |
      | false                           |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "a") |
      | false                         |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'A'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "A") |
      | true                          |
    When executing query:
      """
      YIELD 'apple' NOT STARTS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" NOT STARTS WITH "b") |
      | true                          |
    When executing query:
      """
      YIELD '123' NOT STARTS WITH '1'
      """
    Then the result should be, in any order:
      | ("123" NOT STARTS WITH "1") |
      | false                       |
    When executing query:
      """
      YIELD 123 NOT STARTS WITH 1
      """
    Then a SemanticError should be raised at runtime: `1', expected String, but wat INT

  Scenario: starts with
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst STARTS WITH 'LaMarcus' YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst STARTS WITH 'Obama' YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER * WHERE dst(edge) STARTS WITH 'S' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                    |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}] |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER * WHERE $$.player.name STARTS WITH 'T' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                             |
      | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}] |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                         |
    When executing query:
      """
      GO 2 STEPS FROM 'Tim Duncan' OVER * WHERE $$.player.name STARTS WITH 'M' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                                |
      | [:like "Danny Green"->"Marco Belinelli" @0 {likeness: 83}]                       |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                         |
      | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}] |
    When executing query:
      """
      MATCH p = (a:player)-[*..2]-(b) WHERE a.player.age > 40 AND b.player.name STARTS WITH "Y" RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                                                                                                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})<-[:like@0 {likeness: 80}]-("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})> |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                               |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})<-[:like@0 {likeness: 90}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                               |
      | <("Vince Carter" :player{age: 42, name: "Vince Carter"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                           |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})<-[:like@0 {likeness: 90}]-("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                                                                                                       |
    When executing query:
      """
      MATCH p = (a:player)-[]->(b) WHERE a.player.name STARTS WITH "Y" RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})>       |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})> |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})>         |

  Scenario: not starts with
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst NOT STARTS WITH 'T' YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
      | 'Tony Parker'  |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like
      WHERE like.likeness NOT IN [95,56,21] AND $$.player.name NOT STARTS WITH 'Tony'
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name      | $$.player.name | like.likeness |
      | 'Manu Ginobili'     | 'Tim Duncan'   | 90            |
      | 'LaMarcus Aldridge' | 'Tim Duncan'   | 75            |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like
      WHERE like.likeness NOT IN [95,56,21] AND $^.player.name NOT STARTS WITH 'LaMarcus'
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name  | $$.player.name | like.likeness |
      | 'Manu Ginobili' | 'Tim Duncan'   | 90            |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER * WHERE dst(edge) NOT STARTS WITH 'S' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                                    |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]                         |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                                |
      | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]              |
      | [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]     |
      | [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}] |
      | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]     |
      | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER * WHERE $$.player.name NOT STARTS WITH 'T' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                                    |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]                         |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]     |
      | [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}] |
      | [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]     |
    When executing query:
      """
      GO 2 STEPS FROM 'Tim Duncan' OVER * WHERE dst(edge) NOT STARTS WITH 'M' AND dst(edge) NOT STARTS WITH 'T' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                                    |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]                         |
      | [:serve "Tony Parker"->"Hornets" @0 {end_year: 2019, start_year: 2018}]              |
      | [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]                |
      | [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]     |
      | [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}] |
      | [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]          |
      | [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]              |
      | [:like "Danny Green"->"LeBron James" @0 {likeness: 80}]                              |
      | [:serve "Danny Green"->"Cavaliers" @0 {end_year: 2010, start_year: 2009}]            |
      | [:serve "Danny Green"->"Raptors" @0 {end_year: 2019, start_year: 2018}]              |
      | [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]                |
    When executing query:
      """
      MATCH p = (a:player)-[*..2]->(b) WHERE a.player.age > 45 AND b.player.name NOT STARTS WITH "T" RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                                                                                                                                                           |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:like@0 {likeness: 90}]->("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})>                                                                                                   |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2015}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:like@0 {likeness: 90}]->("Rudy Gay" :player{age: 32, name: "Rudy Gay"})>                                                                                                         |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                 |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>         |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:like@0 {likeness: 90}]->("Grant Hill" :player{age: 46, name: "Grant Hill"})>                                                                                                     |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 100}]->("JaVale McGee" :player{age: 31, name: "JaVale McGee"})>                                                                                                                                                                        |
      | <("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})-[:like@0 {likeness: 80}]->("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2010}]->("Danny Green" :player{age: 31, name: "Danny Green"})>             |
    When executing query:
      """
      MATCH p = (a:player)-[]->(b) WHERE a.player.name NOT STARTS WITH "Y" RETURN p ORDER BY p Limit 5
      """
    Then the result should be, in any order:
      | p                                                                                                                                                   |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})-[:serve@0 {end_year: 2016, start_year: 2015}]->("Heat" :team{name: "Heat"})>     |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})-[:serve@0 {end_year: 2015, start_year: 2010}]->("Knicks" :team{name: "Knicks"})> |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})-[:like@0 {likeness: 90}]->("Steve Nash" :player{age: 45, name: "Steve Nash"})>   |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})-[:serve@0 {end_year: 2010, start_year: 2002}]->("Suns" :team{name: "Suns"})>     |
      | <("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:serve@0 {end_year: 2019, start_year: 2017}]->("Celtics" :team{name: "Celtics"})>           |

  Scenario: yield ends with
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'le'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "le") |
      | true                     |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "app") |
      | false                     |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "a") |
      | false                   |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'e'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "e") |
      | true                    |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'E'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "E") |
      | false                   |
    When executing query:
      """
      YIELD 'apple' ENDS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" ENDS WITH "b") |
      | false                   |
    When executing query:
      """
      YIELD '123' ENDS WITH '3'
      """
    Then the result should be, in any order:
      | ("123" ENDS WITH "3") |
      | true                  |
    When executing query:
      """
      YIELD 123 ENDS WITH 3
      """
    Then a SemanticError should be raised at runtime: `3', expected String, but wat INT

  Scenario: yield not ends with
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'le'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "le") |
      | false                        |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'app'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "app") |
      | true                          |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'a'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "a") |
      | true                        |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'e'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "e") |
      | false                       |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'E'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "E") |
      | true                        |
    When executing query:
      """
      YIELD 'apple' NOT ENDS WITH 'b'
      """
    Then the result should be, in any order:
      | ("apple" NOT ENDS WITH "b") |
      | true                        |
    When executing query:
      """
      YIELD '123' NOT ENDS WITH '3'
      """
    Then the result should be, in any order:
      | ("123" NOT ENDS WITH "3") |
      | false                     |
    When executing query:
      """
      YIELD 123 NOT ENDS WITH 3
      """
    Then a SemanticError should be raised at runtime: `3', expected String, but wat INT

  Scenario: ends with
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst ENDS WITH 'Ginobili' YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER * WHERE dst(edge) ENDS WITH 'r' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                             |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]                         |
      | [:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}] |
    When executing query:
      """
      GO 2 STEPS FROM 'Tim Duncan' OVER * WHERE $$.player.name ENDS WITH 's' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                       |
      | [:like "Danny Green"->"LeBron James" @0 {likeness: 80}] |
    When executing query:
      """
      MATCH p = (a:player)-[]->(b) WHERE a.player.name ENDS WITH "g" RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                               |
      | <("Kyrie Irving" :player{age: 26, name: "Kyrie Irving"})-[:serve@0 {end_year: 2017, start_year: 2011}]->("Cavaliers" :team{name: "Cavaliers"})> |
      | <("Kyrie Irving" :player{age: 26, name: "Kyrie Irving"})-[:serve@0 {end_year: 2019, start_year: 2017}]->("Celtics" :team{name: "Celtics"})>     |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Shaquille O'Neal" :player{age: 47, name: "Shaquille O'Neal"})>     |
      | <("Kyrie Irving" :player{age: 26, name: "Kyrie Irving"})-[:like@0 {likeness: 13}]->("LeBron James" :player{age: 34, name: "LeBron James"})>     |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:serve@0 {end_year: 2011, start_year: 2002}]->("Rockets" :team{name: "Rockets"})>             |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})>           |

  Scenario: not ends with
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like WHERE like._dst NOT ENDS WITH 'Ginobili' YIELD $^.player.name
      """
    Then the result should be, in any order:
      | $^.player.name |
      | 'Tony Parker'  |
      | 'Tony Parker'  |
    When executing query:
      """
      GO FROM 'Tim Duncan' OVER * WHERE dst(edge) NOT ENDS WITH 'r' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                                   |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                |
      | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]       |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}] |
      | [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]     |
    When executing query:
      """
      GO 2 STEPS FROM 'Tim Duncan' OVER * WHERE $$.player.name NOT ENDS WITH 's' YIELD edge as e
      """
    Then the result should be, in any order:
      | e                                                                                    |
      | [:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]                         |
      | [:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]                             |
      | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]                                |
      | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]                                |
      | [:like "Danny Green"->"Marco Belinelli" @0 {likeness: 83}]                           |
      | [:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]     |
      | [:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}] |
      | [:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]     |
      | [:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]        |
      | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]                          |
      | [:like "LaMarcus Aldridge"->"Tony Parker" @0 {likeness: 75}]                         |
      | [:teammate "Manu Ginobili"->"Tony Parker" @0 {end_year: 2016, start_year: 2002}]     |
      | [:teammate "Manu Ginobili"->"Tim Duncan" @0 {end_year: 2016, start_year: 2002}]      |
      | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]                              |
    When executing query:
      """
      MATCH p = (a:player)-[]->(b) WHERE a.player.name NOT ENDS WITH "g" RETURN p ORDER BY p Limit 5
      """
    Then the result should be, in any order:
      | p                                                                                                                                                   |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})-[:serve@0 {end_year: 2016, start_year: 2015}]->("Heat" :team{name: "Heat"})>     |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})-[:serve@0 {end_year: 2015, start_year: 2010}]->("Knicks" :team{name: "Knicks"})> |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})-[:like@0 {likeness: 90}]->("Steve Nash" :player{age: 45, name: "Steve Nash"})>   |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})-[:serve@0 {end_year: 2010, start_year: 2002}]->("Suns" :team{name: "Suns"})>     |
      | <("Aron Baynes" :player{age: 32, name: "Aron Baynes"})-[:serve@0 {end_year: 2019, start_year: 2017}]->("Celtics" :team{name: "Celtics"})>           |

  Scenario: contains
    When executing query:
      """
      GO FROM "Tim Duncan" OVER * WHERE dst(edge) CONTAINS 'r' YIELD edge AS e
      """
    Then the result should be, in any order:
      | e                                                                                   |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]                               |
      | [:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]                |
      | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]       |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}] |
      | [:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]       |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER * WHERE $$.player.name CONTAINS 'r' YIELD edge AS e
      """
    Then the result should be, in any order:
      | e                                                                                   |
      | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]                               |
      | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]       |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}] |
      | [:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]       |
    When executing query:
      """
      MATCH p = (a:player)-[]->(b) WHERE a.player.name CONTAINS "z" RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                           |
      | <("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"})-[:like@0 {likeness: 90}]->("Luka Doncic" :player{age: 20, name: "Luka Doncic"})>       |
      | <("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})-[:like@0 {likeness: 80}]->("Jason Kidd" :player{age: 45, name: "Jason Kidd"})>                   |
      | <("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})-[:like@0 {likeness: 10}]->("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})>                 |
      | <("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})-[:serve@0 {end_year: 2019, start_year: 1998}]->("Mavericks" :team{name: "Mavericks"})>           |
      | <("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"})-[:serve@0 {end_year: 2020, start_year: 2019}]->("Mavericks" :team{name: "Mavericks"})> |
      | <("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"})-[:serve@0 {end_year: 2019, start_year: 2015}]->("Knicks" :team{name: "Knicks"})>       |
      | <("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})-[:like@0 {likeness: 80}]->("Steve Nash" :player{age: 45, name: "Steve Nash"})>                   |

  Scenario: not contains
    When executing query:
      """
      GO FROM "Tim Duncan" OVER * WHERE dst(edge) NOT CONTAINS 'r' YIELD edge AS e
      """
    Then the result should be, in any order:
      | e                                                                               |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]                         |
      | [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}] |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER * WHERE $$.player.name NOT CONTAINS 'r' YIELD edge AS e
      """
    Then the result should be, in any order:
      | e                                                                               |
      | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]                         |
      | [:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}] |
    When executing query:
      """
      MATCH p = (a:player)-[]->(b) WHERE a.player.name NOT CONTAINS "a" RETURN p ORDER BY p LIMIT 5
      """
    Then the result should be, in any order:
      | p                                                                                                                                         |
      | <("Ben Simmons" :player{age: 22, name: "Ben Simmons"})-[:serve@0 {end_year: 2019, start_year: 2016}]->("76ers" :team{name: "76ers"})>     |
      | <("Ben Simmons" :player{age: 22, name: "Ben Simmons"})-[:like@0 {likeness: 80}]->("Joel Embiid" :player{age: 25, name: "Joel Embiid"})>   |
      | <("Cory Joseph" :player{age: 27, name: "Cory Joseph"})-[:serve@0 {end_year: 2019, start_year: 2017}]->("Pacers" :team{name: "Pacers"})>   |
      | <("Cory Joseph" :player{age: 27, name: "Cory Joseph"})-[:serve@0 {end_year: 2017, start_year: 2015}]->("Raptors" :team{name: "Raptors"})> |
      | <("Cory Joseph" :player{age: 27, name: "Cory Joseph"})-[:serve@0 {end_year: 2015, start_year: 2011}]->("Spurs" :team{name: "Spurs"})>     |

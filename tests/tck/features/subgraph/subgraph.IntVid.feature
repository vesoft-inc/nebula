# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Integer Vid subgraph

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: Integer Vid invalid input
    When executing query:
      """
      GET SUBGRAPH WITH PROP FROM $-.id
      """
    Then a SemanticError should be raised at runtime: `$-.id', not exist prop `id'
    When executing query:
      """
      GET SUBGRAPH WITH PROP FROM $a.id
      """
    Then a SemanticError should be raised at runtime: `$a.id', not exist variable `a'
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like YIELD $$.player.name AS id | GET SUBGRAPH WITH PROP FROM $-.id
      """
    Then a SemanticError should be raised at runtime: `$-.id', the srcs should be type of INT64, but was`STRING'
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") OVER like YIELD $$.player.name AS ID; GET SUBGRAPH WITH PROP FROM $a.ID
      """
    Then a SemanticError should be raised at runtime: `$a.ID', the srcs should be type of INT64, but was`STRING'
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") OVER like YIELD like._src AS src; GET SUBGRAPH WITH PROP FROM $b.src
      """
    Then a SemanticError should be raised at runtime: `$b.src', not exist variable `b'
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS id, like._src AS id | GET SUBGRAPH WITH PROP FROM $-.id
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS id, like._src AS id; GET SUBGRAPH WITH PROP FROM $a.id
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'

  Scenario: Integer Vid zero step
    When executing query:
      """
      GET SUBGRAPH WITH PROP 0 STEPS FROM hash("Tim Duncan")
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices        |
      | [("Tim Duncan")] |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 0 STEPS FROM hash("Tim Duncan"), hash("Spurs")
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                   |
      | [("Tim Duncan"), ("Spurs")] |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 0 STEPS FROM hash("Tim Duncan"), hash("Tony Parker"), hash("Spurs")
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                                    |
      | [("Tim Duncan"), ("Spurs"), ("Tony Parker")] |
    When executing query:
      """
      GO FROM hash('Tim Duncan') over serve YIELD serve._dst AS id | GET SUBGRAPH WITH PROP 0 STEPS FROM $-.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices   |
      | [("Spurs")] |
    When executing query:
      """
      GO FROM hash('Tim Duncan') over like YIELD like._dst AS id | GET SUBGRAPH WITH PROP 0 STEPS FROM $-.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                            |
      | [("Manu Ginobili"), ("Tony Parker")] |
    When executing query:
      """
      $a = GO FROM hash('Tim Duncan') over serve YIELD serve._dst AS id; GET SUBGRAPH WITH PROP 0 STEPS FROM $a.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices   |
      | [("Spurs")] |
    When executing query:
      """
      $a = GO FROM hash('Tim Duncan') over like YIELD like._dst AS id; GET SUBGRAPH WITH PROP 0 STEPS FROM $a.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                            |
      | [("Manu Ginobili"), ("Tony Parker")] |

  Scenario: Integer Vid subgraph
    When executing query:
      """
      GET SUBGRAPH WITH PROP FROM hash('Tim Duncan')
      """
    Then define some list variables:
      | edge1                                           | vertex2               | edge2                                            |
      | [:teammate "Manu Ginobili"->"Tim Duncan"@0]     | ("Danny Green")       | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      | [:teammate "Tony Parker"->"Tim Duncan"@0]       | ("Manu Ginobili")     | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |
      | [:like "Aron Baynes"->"Tim Duncan"@0]           | ("Aron Baynes")       | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |
      | [:like "Boris Diaw"->"Tim Duncan"@0]            | ("Boris Diaw")        | [:like "Tony Parker"->"Manu Ginobili"@0]         |
      | [:like "Danny Green"->"Tim Duncan"@0]           | ("Shaquile O\'Neal")  | [:serve "Manu Ginobili"->"Spurs"@0]              |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]       | ("Tony Parker")       | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]     | ("Spurs")             | [:serve "Aron Baynes"->"Spurs"@0]                |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]         | ("Dejounte Murray")   | [:like "Boris Diaw"->"Tony Parker"@0]            |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:serve "Boris Diaw"->"Spurs"@0]                 |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]      | ("Marco Belinelli")   | [:like "Dejounte Murray"->"Tony Parker"@0]       |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]        | ("Tiago Splitter")    | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     |
      | [:like "Tony Parker"->"Tim Duncan"@0]           |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]         |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     |
      | [:like "Tim Duncan"->"Tony Parker"@0]           |                       | [:serve "Tony Parker"->"Spurs"@0]                |
      | [:serve "Tim Duncan"->"Spurs"@0]                |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      | [:teammate "Tim Duncan"->"Danny Green"@0]       |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0] |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          |
      | [:teammate "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]       |                       | [:serve "Tiago Splitter"->"Spurs"@0]             |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |
      |                                                 |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |
      |                                                 |                       | [:like "Dejounte Murray"->"Danny Green"@0]       |
      |                                                 |                       | [:like "Marco Belinelli"->"Danny Green"@0]       |
      |                                                 |                       | [:like "Danny Green"->"Marco Belinelli"@0]       |
      |                                                 |                       | [:serve "Danny Green"->"Spurs"@0]                |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |

  Scenario: Integer Vid two steps
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM hash('Tim Duncan')
      """
    Then define some list variables:
      | edge1                                           | vertex2               | edge2                                            | vertex3               | edge3                                         |
      | [:teammate "Manu Ginobili"->"Tim Duncan"@0]     | ("Danny Green")       | [:like "Dejounte Murray"->"Danny Green"@0]       | ("Cavaliers")         | [:serve "LeBron James"->"Cavaliers"@0]        |
      | [:teammate "Tony Parker"->"Tim Duncan"@0]       | ("Manu Ginobili")     | [:like "Marco Belinelli"->"Danny Green"@0]       | ("Pistons")           | [:serve "LeBron James"->"Cavaliers"@1]        |
      | [:like "Aron Baynes"->"Tim Duncan"@0]           | ("Aron Baynes")       | [:like "Danny Green"->"LeBron James"@0]          | ("Damian Lillard")    | [:serve "Damian Lillard"->"Trail Blazers"@0]  |
      | [:like "Boris Diaw"->"Tim Duncan"@0]            | ("Boris Diaw")        | [:like "Danny Green"->"Marco Belinelli"@0]       | ("Kings")             | [:serve "Rudy Gay"->"Kings"@0]                |
      | [:like "Danny Green"->"Tim Duncan"@0]           | ("Shaquile O\'Neal")  | [:serve "Danny Green"->"Cavaliers"@0]            | ("Raptors")           | [:serve "Cory Joseph"->"Raptors"@0]           |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]       | ("Tony Parker")       | [:serve "Danny Green"->"Raptors"@0]              | ("Jazz")              | [:serve "Rudy Gay"->"Raptors"@0]              |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]     | ("Spurs")             | [:serve "Danny Green"->"Spurs"@0]                | ("LeBron James")      | [:serve "Tracy McGrady"->"Raptors"@0]         |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]         | ("Dejounte Murray")   | [:teammate "Tony Parker"->"Manu Ginobili"@0]     | ("Paul Gasol")        | [:like "Chris Paul"->"LeBron James"@0]        |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:like "Dejounte Murray"->"Manu Ginobili"@0]     | ("Kyle Anderson")     | [:serve "LeBron James"->"Heat"@0]             |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]      | ("Marco Belinelli")   | [:like "Tiago Splitter"->"Manu Ginobili"@0]      | ("Rudy Gay")          | [:serve "LeBron James"->"Lakers"@0]           |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]        | ("Tiago Splitter")    | [:like "Tony Parker"->"Manu Ginobili"@0]         | ("Kevin Durant")      | [:serve "Paul Gasol"->"Bulls"@0]              |
      | [:like "Tony Parker"->"Tim Duncan"@0]           |                       | [:serve "Manu Ginobili"->"Spurs"@0]              | ("Yao Ming")          | [:serve "Paul Gasol"->"Lakers"@0]             |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]         |                       | [:teammate "Manu Ginobili"->"Tony Parker"@0]     | ("James Harden")      | [:like "Tracy McGrady"->"Rudy Gay"@0]         |
      | [:like "Tim Duncan"->"Tony Parker"@0]           |                       | [:serve "Aron Baynes"->"Celtics"@0]              | ("Hornets")           | [:serve "Kevin Durant"->"Warriors"@0]         |
      | [:serve "Tim Duncan"->"Spurs"@0]                |                       | [:serve "Aron Baynes"->"Pistons"@0]              | ("David West")        | [:like "Yao Ming"->"Tracy McGrady"@0]         |
      | [:teammate "Tim Duncan"->"Danny Green"@0]       |                       | [:serve "Aron Baynes"->"Spurs"@0]                | ("Chris Paul")        | [:like "Russell Westbrook"->"James Harden"@0] |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0] |                       | [:like "Boris Diaw"->"Tony Parker"@0]            | ("Celtics")           | [:like "James Harden"->"Russell Westbrook"@0] |
      | [:teammate "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:serve "Boris Diaw"->"Hawks"@0]                 | ("Jonathon Simmons")  | [:serve "Chris Paul"->"Hornets"@0]            |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]       |                       | [:serve "Boris Diaw"->"Hornets"@0]               | ("Hawks")             | [:serve "David West"->"Hornets"@0]            |
      |                                                 |                       | [:serve "Boris Diaw"->"Jazz"@0]                  | ("Heat")              | [:serve "David West"->"Warriors"@0]           |
      |                                                 |                       | [:serve "Boris Diaw"->"Spurs"@0]                 | ("Lakers")            | [:serve "Jonathon Simmons"->"76ers"@0]        |
      |                                                 |                       | [:serve "Boris Diaw"->"Suns"@0]                  | ("Suns")              | [:serve "Jonathon Simmons"->"Magic"@0]        |
      |                                                 |                       | [:like "Yao Ming"->"Shaquile O\'Neal"@0]         | ("Magic")             | [:serve "JaVale McGee"->"Lakers"@0]           |
      |                                                 |                       | [:like "Shaquile O\'Neal"->"JaVale McGee"@0]     | ("Trail Blazers")     | [:serve "Tracy McGrady"->"Magic"@0]           |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Cavaliers"@0]       | ("76ers")             | [:serve "JaVale McGee"->"Warriors"@0]         |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Celtics"@0]         | ("JaVale McGee")      |                                               |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Heat"@0]            | ("Cory Joseph")       |                                               |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Lakers"@0]          | ("Tracy McGrady")     |                                               |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Magic"@0]           | ("Russell Westbrook") |                                               |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Suns"@0]            | ("Bulls")             |                                               |
      |                                                 |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       | ("Warriors")          |                                               |
      |                                                 |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     |                       |                                               |
      |                                                 |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |                       |                                               |
      |                                                 |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     |                       |                                               |
      |                                                 |                       | [:serve "Tony Parker"->"Hornets"@0]              |                       |                                               |
      |                                                 |                       | [:serve "Tony Parker"->"Spurs"@0]                |                       |                                               |
      |                                                 |                       | [:teammate "Tony Parker"->"Kyle Anderson"@0]     |                       |                                               |
      |                                                 |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |                       |                                               |
      |                                                 |                       | [:serve "Cory Joseph"->"Spurs"@0]                |                       |                                               |
      |                                                 |                       | [:serve "David West"->"Spurs"@0]                 |                       |                                               |
      |                                                 |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |                       |                                               |
      |                                                 |                       | [:serve "Jonathon Simmons"->"Spurs"@0]           |                       |                                               |
      |                                                 |                       | [:serve "Kyle Anderson"->"Spurs"@0]              |                       |                                               |
      |                                                 |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |                       |                                               |
      |                                                 |                       | [:serve "Paul Gasol"->"Spurs"@0]                 |                       |                                               |
      |                                                 |                       | [:serve "Rudy Gay"->"Spurs"@0]                   |                       |                                               |
      |                                                 |                       | [:serve "Tiago Splitter"->"Spurs"@0]             |                       |                                               |
      |                                                 |                       | [:serve "Tracy McGrady"->"Spurs"@0]              |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |                       |                                               |
      |                                                 |                       | [:like "Dejounte Murray"->"Chris Paul"@0]        |                       |                                               |
      |                                                 |                       | [:like "Dejounte Murray"->"James Harden"@0]      |                       |                                               |
      |                                                 |                       | [:like "Dejounte Murray"->"Kevin Durant"@0]      |                       |                                               |
      |                                                 |                       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     |                       |                                               |
      |                                                 |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       |                                               |
      |                                                 |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |                       |                                               |
      |                                                 |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       |                                               |
      |                                                 |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  |                       |                                               |
      |                                                 |                       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]        |                       |                                               |
      |                                                 |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0]  |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"76ers"@0]            |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Bulls"@0]            |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Hawks"@0]            |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Hornets"@0]          |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Kings"@0]            |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Raptors"@0]          |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Warriors"@0]         |                       |                                               |
      |                                                 |                       | [:serve "Marco Belinelli"->"Hornets"@1]          |                       |                                               |
      |                                                 |                       | [:serve "Tiago Splitter"->"76ers"@0]             |                       |                                               |
      |                                                 |                       | [:serve "Tiago Splitter"->"Hawks"@0]             |                       |                                               |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | <[edge3]> |

  Scenario: Integer Vid in edge
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM hash('Tim Duncan') IN like, serve
      """
    Then define some list variables:
      | edge1                                       | vertex2               | edge2                                           | vertex3            |
      | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:like "Damian Lillard"->"LaMarcus Aldridge"@0] | ("Damian Lillard") |
      | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Danny Green")       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]       | ("Yao Ming")       |
      | [:like "Danny Green"->"Tim Duncan"@0]       | ("Marco Belinelli")   | [:like "Tony Parker"->"LaMarcus Aldridge"@0]    | ("Rudy Gay")       |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]   | ("Manu Ginobili")     | [:like "Dejounte Murray"->"Danny Green"@0]      |                    |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] | ("Shaquile O'Neal")   | [:like "Marco Belinelli"->"Danny Green"@0]      |                    |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]     | ("Tony Parker")       | [:like "Danny Green"->"Marco Belinelli"@0]      |                    |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]   | ("Boris Diaw")        | [:like "Dejounte Murray"->"Marco Belinelli"@0]  |                    |
      | [:like "Shaquile O'Neal"->"Tim Duncan"@0]   | ("Dejounte Murray")   | [:like "Dejounte Murray"->"Manu Ginobili"@0]    |                    |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]    | ("Aron Baynes")       | [:like "Tiago Splitter"->"Manu Ginobili"@0]     |                    |
      | [:like "Tony Parker"->"Tim Duncan"@0]       | ("Tiago Splitter")    | [:like "Tim Duncan"->"Manu Ginobili"@0]         |                    |
      |                                             |                       | [:like "Tony Parker"->"Manu Ginobili"@0]        |                    |
      |                                             |                       | [:like "Yao Ming"->"Shaquile O'Neal"@0]         |                    |
      |                                             |                       | [:like "Boris Diaw"->"Tony Parker"@0]           |                    |
      |                                             |                       | [:like "Dejounte Murray"->"Tony Parker"@0]      |                    |
      |                                             |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]    |                    |
      |                                             |                       | [:like "Marco Belinelli"->"Tony Parker"@0]      |                    |
      |                                             |                       | [:like "Tim Duncan"->"Tony Parker"@0]           |                    |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | []        |

  Scenario: Integer Vid in and out edge
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM hash('Tim Duncan') IN like OUT serve
      """
    Then define some list variables:
      | edge1                                       | vertex2               | edge2                                           | vertex3            | edge3                                        |
      | [:serve "Tim Duncan"->"Spurs"@0]            | ("LaMarcus Aldridge") | [:serve "LaMarcus Aldridge"->"Spurs"@0]         | ("Damian Lillard") | [:serve "Damian Lillard"->"Trail Blazers"@0] |
      | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("Danny Green")       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0] | ("Rudy Gay")       | [:serve "Rudy Gay"->"Spurs"@0]               |
      | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Marco Belinelli")   | [:like "Damian Lillard"->"LaMarcus Aldridge"@0] | ("Hornets")        | [:serve "Rudy Gay"->"Raptors"@0]             |
      | [:like "Danny Green"->"Tim Duncan"@0]       | ("Boris Diaw")        | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]       | ("Heat")           | [:serve "Rudy Gay"->"Kings"@0]               |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]   | ("Dejounte Murray")   | [:like "Tony Parker"->"LaMarcus Aldridge"@0]    | ("76ers")          |                                              |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] | ("Aron Baynes")       | [:serve "Danny Green"->"Cavaliers"@0]           | ("Bulls")          |                                              |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]     | ("Manu Ginobili")     | [:serve "Danny Green"->"Raptors"@0]             | ("Trail Blazers")  |                                              |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]   | ("Tiago Splitter")    | [:serve "Danny Green"->"Spurs"@0]               | ("Celtics")        |                                              |
      | [:like "Shaquile O'Neal"->"Tim Duncan"@0]   | ("Shaquile O'Neal")   | [:like "Dejounte Murray"->"Danny Green"@0]      | ("Kings")          |                                              |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]    | ("Tony Parker")       | [:like "Marco Belinelli"->"Danny Green"@0]      | ("Hawks")          |                                              |
      | [:like "Tony Parker"->"Tim Duncan"@0]       | ("Spurs")             | [:serve "Marco Belinelli"->"76ers"@0]           | ("Warriors")       |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Bulls"@0]           | ("Cavaliers")      |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Hawks"@0]           | ("Raptors")        |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Hornets"@0]         | ("Jazz")           |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Kings"@0]           | ("Pistons")        |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Raptors"@0]         | ("Lakers")         |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Spurs"@0]           | ("Suns")           |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Warriors"@0]        | ("Magic")          |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Hornets"@1]         | ("Yao Ming")       |                                              |
      |                                             |                       | [:serve "Marco Belinelli"->"Spurs"@1]           |                    |                                              |
      |                                             |                       | [:like "Danny Green"->"Marco Belinelli"@0]      |                    |                                              |
      |                                             |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]  |                    |                                              |
      |                                             |                       | [:serve "Boris Diaw"->"Hawks"@0]                |                    |                                              |
      |                                             |                       | [:serve "Boris Diaw"->"Hornets"@0]              |                    |                                              |
      |                                             |                       | [:serve "Boris Diaw"->"Jazz"@0]                 |                    |                                              |
      |                                             |                       | [:serve "Boris Diaw"->"Spurs"@0]                |                    |                                              |
      |                                             |                       | [:serve "Boris Diaw"->"Suns"@0]                 |                    |                                              |
      |                                             |                       | [:serve "Dejounte Murray"->"Spurs"@0]           |                    |                                              |
      |                                             |                       | [:serve "Aron Baynes"->"Celtics"@0]             |                    |                                              |
      |                                             |                       | [:serve "Aron Baynes"->"Pistons"@0]             |                    |                                              |
      |                                             |                       | [:serve "Aron Baynes"->"Spurs"@0]               |                    |                                              |
      |                                             |                       | [:serve "Manu Ginobili"->"Spurs"@0]             |                    |                                              |
      |                                             |                       | [:like "Dejounte Murray"->"Manu Ginobili"@0]    |                    |                                              |
      |                                             |                       | [:like "Tiago Splitter"->"Manu Ginobili"@0]     |                    |                                              |
      |                                             |                       | [:like "Tim Duncan"->"Manu Ginobili"@0]         |                    |                                              |
      |                                             |                       | [:like "Tony Parker"->"Manu Ginobili"@0]        |                    |                                              |
      |                                             |                       | [:serve "Tiago Splitter"->"76ers"@0]            |                    |                                              |
      |                                             |                       | [:serve "Tiago Splitter"->"Hawks"@0]            |                    |                                              |
      |                                             |                       | [:serve "Tiago Splitter"->"Spurs"@0]            |                    |                                              |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Cavaliers"@0]       |                    |                                              |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Celtics"@0]         |                    |                                              |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Heat"@0]            |                    |                                              |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Lakers"@0]          |                    |                                              |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Magic"@0]           |                    |                                              |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Suns"@0]            |                    |                                              |
      |                                             |                       | [:like "Yao Ming"->"Shaquile O'Neal"@0]         |                    |                                              |
      |                                             |                       | [:serve "Tony Parker"->"Hornets"@0]             |                    |                                              |
      |                                             |                       | [:serve "Tony Parker"->"Spurs"@0]               |                    |                                              |
      |                                             |                       | [:like "Boris Diaw"->"Tony Parker"@0]           |                    |                                              |
      |                                             |                       | [:like "Dejounte Murray"->"Tony Parker"@0]      |                    |                                              |
      |                                             |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]    |                    |                                              |
      |                                             |                       | [:like "Marco Belinelli"->"Tony Parker"@0]      |                    |                                              |
      |                                             |                       | [:like "Tim Duncan"->"Tony Parker"@0]           |                    |                                              |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | <[edge3]> |

  Scenario: Integer Vid two steps in and out edge
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM hash('Tim Duncan'), hash('James Harden') IN teammate OUT serve
      """
    Then define some list variables:
      | vertex1          | edge1                                       | vertex2           | edge2                                        | vertex3     |
      | ("Tim Duncan")   | [:serve "Tim Duncan"->"Spurs"@0]            | ("Manu Ginobili") | [:serve "Manu Ginobili"->"Spurs"@0]          | ("Hornets") |
      | ("James Harden") | [:teammate "Manu Ginobili"->"Tim Duncan"@0] | ("Tony Parker")   | [:teammate "Tim Duncan"->"Manu Ginobili"@0]  |             |
      |                  | [:teammate "Tony Parker"->"Tim Duncan"@0]   | ("Spurs")         | [:teammate "Tony Parker"->"Manu Ginobili"@0] |             |
      |                  | [:serve "James Harden"->"Rockets"@0]        | ("Rockets")       | [:serve "Tony Parker"->"Hornets"@0]          |             |
      |                  | [:serve "James Harden"->"Thunders"@0]       | ("Thunders")      | [:serve "Tony Parker"->"Spurs"@0]            |             |
      |                  |                                             |                   | [:teammate "Manu Ginobili"->"Tony Parker"@0] |             |
      |                  |                                             |                   | [:teammate "Tim Duncan"->"Tony Parker"@0]    |             |
    Then the result should be, in any order, with relax comparison:
      | _vertices   | _edges    |
      | <[vertex1]> | <[edge1]> |
      | <[vertex2]> | <[edge2]> |
      | <[vertex3]> | []        |

  Scenario: Integer Vid three steps
    When executing query:
      """
      GET SUBGRAPH WITH PROP 3 STEPS FROM hash('Paul George') OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                        | edge2                                            | edge3                                          | vertex4             | edge4                                      |
      | [:like "Russell Westbrook"->"Paul George"@0] | [:like "Dejounte Murray"->"Russell Westbrook"@0] | [:serve "Dejounte Murray"->"Spurs"@0]          | ("Kyle Anderson")   | [:like "Tony Parker"->"Tim Duncan"@0]      |
      | [:serve "Paul George"->"Pacers"@0]           | [:like "James Harden"->"Russell Westbrook"@0]    | [:like "Dejounte Murray"->"Chris Paul"@0]      | ("Tony Parker")     | [:serve "Kyle Anderson"->"Spurs"@0]        |
      | [:serve "Paul George"->"Thunders"@0]         | [:serve "Russell Westbrook"->"Thunders"@0]       | [:like "Dejounte Murray"->"Danny Green"@0]     | ("Danny Green")     | [:like "Marco Belinelli"->"Danny Green"@0] |
      | [:like "Paul George"->"Russell Westbrook"@0] | [:like "Russell Westbrook"->"James Harden"@0]    | [:like "Dejounte Murray"->"James Harden"@0]    | ("Luka Doncic")     | [:like "Tony Parker"->"Manu Ginobili"@0]   |
      |                                              |                                                  | [:like "Dejounte Murray"->"Kevin Durant"@0]    | ("Tim Duncan")      | [:serve "Tony Parker"->"Spurs"@0]          |
      |                                              |                                                  | [:like "Dejounte Murray"->"Kyle Anderson"@0]   | ("Marco Belinelli") | [:serve "Danny Green"->"Spurs"@0]          |
      |                                              |                                                  | [:like "Dejounte Murray"->"LeBron James"@0]    | ("Kevin Durant")    | [:like "Danny Green"->"LeBron James"@0]    |
      |                                              |                                                  | [:like "Dejounte Murray"->"Manu Ginobili"@0]   | ("Manu Ginobili")   | [:like "Danny Green"->"Marco Belinelli"@0] |
      |                                              |                                                  | [:like "Dejounte Murray"->"Marco Belinelli"@0] | ("Chris Paul")      | [:like "Danny Green"->"Tim Duncan"@0]      |
      |                                              |                                                  | [:like "Dejounte Murray"->"Tim Duncan"@0]      | ("LeBron James")    | [:like "Tim Duncan"->"Tony Parker"@0]      |
      |                                              |                                                  | [:like "Dejounte Murray"->"Tony Parker"@0]     | ("Spurs")           | [:like "Marco Belinelli"->"Tony Parker"@0] |
      |                                              |                                                  | [:like "Luka Doncic"->"James Harden"@0]        | ("Rockets")         | [:like "Tim Duncan"->"Manu Ginobili"@0]    |
      |                                              |                                                  | [:serve "James Harden"->"Rockets"@0]           |                     | [:serve "Tim Duncan"->"Spurs"@0]           |
      |                                              |                                                  | [:serve "James Harden"->"Thunders"@0]          |                     | [:like "Marco Belinelli"->"Tim Duncan"@0]  |
      |                                              |                                                  |                                                |                     | [:like "Manu Ginobili"->"Tim Duncan"@0]    |
      |                                              |                                                  |                                                |                     | [:serve "Marco Belinelli"->"Spurs"@0]      |
      |                                              |                                                  |                                                |                     | [:serve "Kevin Durant"->"Thunders"@0]      |
      |                                              |                                                  |                                                |                     | [:serve "Marco Belinelli"->"Spurs"@1]      |
      |                                              |                                                  |                                                |                     | [:serve "Manu Ginobili"->"Spurs"@0]        |
      |                                              |                                                  |                                                |                     | [:serve "Chris Paul"->"Rockets"@0]         |
      |                                              |                                                  |                                                |                     | [:like "Chris Paul"->"LeBron James"@0]     |
    Then the result should be, in any order, with relax comparison:
      | _vertices                                         | _edges    |
      | [("Paul George")]                                 | <[edge1]> |
      | [("Russell Westbrook"), ("Pacers"), ("Thunders")] | <[edge2]> |
      | [("Dejounte Murray"), ("James Harden")]           | <[edge3]> |
      | <[vertex4]>                                       | <[edge4]> |

  Scenario: Integer Vid bidirect edge
    When executing query:
      """
      GET SUBGRAPH WITH PROP FROM hash('Tony Parker') BOTH like
      """
    Then define some list variables:
      | edge1                                        | vertex2               | edge2                                          |
      | [:like "Boris Diaw"->"Tony Parker"@0]        | ("Manu Ginobili")     | [:like "Manu Ginobili"->"Tim Duncan"@0]        |
      | [:like "Dejounte Murray"->"Tony Parker"@0]   | ("Marco Belinelli")   | [:like "Dejounte Murray"->"Marco Belinelli"@0] |
      | [:like "LaMarcus Aldridge"->"Tony Parker"@0] | ("Tim Duncan")        | [:like "Marco Belinelli"->"Tim Duncan"@0]      |
      | [:like "Marco Belinelli"->"Tony Parker"@0]   | ("Dejounte Murray")   | [:like "Tim Duncan"->"Manu Ginobili"@0]        |
      | [:like "Tim Duncan"->"Tony Parker"@0]        | ("LaMarcus Aldridge") | [:like "Boris Diaw"->"Tim Duncan"@0]           |
      | [:like "Tony Parker"->"LaMarcus Aldridge"@0] | ("Boris Diaw")        | [:like "Dejounte Murray"->"Manu Ginobili"@0]   |
      | [:like "Tony Parker"->"Manu Ginobili"@0]     |                       | [:like "Dejounte Murray"->"Tim Duncan"@0]      |
      | [:like "Tony Parker"->"Tim Duncan"@0]        |                       | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]    |
    Then the result should be, in any order, with relax comparison:
      | _vertices         | _edges    |
      | [("Tony Parker")] | <[edge1]> |
      | <[vertex2]>       | <[edge2]> |

  Scenario: Integer Vid pipe
    When executing query:
      """
      GO FROM hash('Tim Duncan') over serve YIELD serve._src AS id | GET SUBGRAPH WITH PROP FROM $-.id
      """
    Then define some list variables:
      | edge1                                           | vertex2               | edge2                                            |
      | [:teammate "Manu Ginobili"->"Tim Duncan"@0]     | ("Danny Green")       | [:like "Dejounte Murray"->"Danny Green"@0]       |
      | [:teammate "Tony Parker"->"Tim Duncan"@0]       | ("Manu Ginobili")     | [:like "Marco Belinelli"->"Danny Green"@0]       |
      | [:like "Aron Baynes"->"Tim Duncan"@0]           | ("Aron Baynes")       | [:like "Danny Green"->"Marco Belinelli"@0]       |
      | [:like "Boris Diaw"->"Tim Duncan"@0]            | ("Boris Diaw")        | [:serve "Danny Green"->"Spurs"@0]                |
      | [:like "Danny Green"->"Tim Duncan"@0]           | ("Shaquile O\'Neal")  | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]       | ("Tony Parker")       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]     | ("Spurs")             | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]         | ("Dejounte Murray")   | [:like "Tony Parker"->"Manu Ginobili"@0]         |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:serve "Manu Ginobili"->"Spurs"@0]              |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]      | ("Marco Belinelli")   | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]        | ("Tiago Splitter")    | [:serve "Aron Baynes"->"Spurs"@0]                |
      | [:like "Tony Parker"->"Tim Duncan"@0]           |                       | [:like "Boris Diaw"->"Tony Parker"@0]            |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]         |                       | [:serve "Boris Diaw"->"Spurs"@0]                 |
      | [:like "Tim Duncan"->"Tony Parker"@0]           |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       |
      | [:serve "Tim Duncan"->"Spurs"@0]                |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     |
      | [:teammate "Tim Duncan"->"Danny Green"@0]       |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0] |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     |
      | [:teammate "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:serve "Tony Parker"->"Spurs"@0]                |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]       |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      |                                                 |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |
      |                                                 |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |
      |                                                 |                       | [:serve "Tiago Splitter"->"Spurs"@0]             |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |
      |                                                 |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |

  Scenario: Integer Vid var
    When executing query:
      """
      $a = GO FROM hash('Tim Duncan') over serve YIELD serve._src AS id;
      GET SUBGRAPH WITH PROP FROM $a.id
      """
    Then define some list variables:
      | edge1                                           | vertex2               | edge2                                            |
      | [:teammate "Manu Ginobili"->"Tim Duncan"@0]     | ("Danny Green")       | [:like "Dejounte Murray"->"Danny Green"@0]       |
      | [:teammate "Tony Parker"->"Tim Duncan"@0]       | ("Manu Ginobili")     | [:like "Marco Belinelli"->"Danny Green"@0]       |
      | [:like "Aron Baynes"->"Tim Duncan"@0]           | ("Aron Baynes")       | [:like "Danny Green"->"Marco Belinelli"@0]       |
      | [:like "Boris Diaw"->"Tim Duncan"@0]            | ("Boris Diaw")        | [:serve "Danny Green"->"Spurs"@0]                |
      | [:like "Danny Green"->"Tim Duncan"@0]           | ("Shaquile O\'Neal")  | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]       | ("Tony Parker")       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]     | ("Spurs")             | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]         | ("Dejounte Murray")   | [:like "Tony Parker"->"Manu Ginobili"@0]         |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:serve "Manu Ginobili"->"Spurs"@0]              |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]      | ("Marco Belinelli")   | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]        | ("Tiago Splitter")    | [:serve "Aron Baynes"->"Spurs"@0]                |
      | [:like "Tony Parker"->"Tim Duncan"@0]           |                       | [:like "Boris Diaw"->"Tony Parker"@0]            |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]         |                       | [:serve "Boris Diaw"->"Spurs"@0]                 |
      | [:like "Tim Duncan"->"Tony Parker"@0]           |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       |
      | [:serve "Tim Duncan"->"Spurs"@0]                |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     |
      | [:teammate "Tim Duncan"->"Danny Green"@0]       |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0] |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     |
      | [:teammate "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:serve "Tony Parker"->"Spurs"@0]                |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]       |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      |                                                 |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |
      |                                                 |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |
      |                                                 |                       | [:serve "Tiago Splitter"->"Spurs"@0]             |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |
      |                                                 |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |

  Scenario: Integer Vid many steps
    When executing query:
      """
      GET SUBGRAPH WITH PROP 4 STEPS FROM hash('Yao Ming') IN teammate OUT serve
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices      | _edges                             |
      | [("Yao Ming")] | [[:serve "Yao Ming"->"Rockets"@0]] |
      | [("Rockets")]  | []                                 |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 4 STEPS FROM hash('NOBODY') IN teammate OUT serve
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices    | _edges |
      | [("NOBODY")] | []     |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 4 steps from hash('Yao Ming') IN teammate OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                   | vertex2             | edge2                                       | vertex3          | edge3                                       | vertex4               | edge4                                            | vertex5               | edge5                                         |
      | [:serve "Yao Ming"->"Rockets"@0]        | ("Shaquile O'Neal") | [:serve "Shaquile O'Neal"->"Cavaliers"@0]   | ("Kobe Bryant")  | [:serve "Kobe Bryant"->"Lakers"@0]          | ("Manu Ginobili")     | [:serve "Manu Ginobili"->"Spurs"@0]              | ("Dirk Nowitzki")     | [:like "Dirk Nowitzki"->"Steve Nash"@0]       |
      | [:like "Yao Ming"->"Shaquile O'Neal"@0] | ("Tracy McGrady")   | [:serve "Shaquile O'Neal"->"Celtics"@0]     | ("Grant Hill")   | [:like "Paul Gasol"->"Kobe Bryant"@0]       | ("Paul Gasol")        | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      | ("Kevin Durant")      | [:serve "Kevin Durant"->"Warriors"@0]         |
      | [:like "Yao Ming"->"Tracy McGrady"@0]   | ("Rockets")         | [:serve "Shaquile O'Neal"->"Heat"@0]        | ("Vince Carter") | [:serve "Grant Hill"->"Clippers"@0]         | ("Jason Kidd")        | [:teammate "Tony Parker"->"Manu Ginobili"@0]     | ("Damian Lillard")    | [:serve "Damian Lillard"->"Trail Blazers"@0]  |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Lakers"@0]      | ("Tim Duncan")   | [:serve "Grant Hill"->"Magic"@0]            | ("Tony Parker")       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     | ("James Harden")      | [:serve "James Harden"->"Rockets"@0]          |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Magic"@0]       | ("JaVale McGee") | [:serve "Grant Hill"->"Pistons"@0]          | ("Marco Belinelli")   | [:like "Tiago Splitter"->"Manu Ginobili"@0]      | ("Chris Paul")        | [:like "Steve Nash"->"Dirk Nowitzki"@0]       |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Suns"@0]        | ("Rudy Gay")     | [:serve "Grant Hill"->"Suns"@0]             | ("Dejounte Murray")   | [:like "Tony Parker"->"Manu Ginobili"@0]         | ("LeBron James")      | [:like "Russell Westbrook"->"James Harden"@0] |
      |                                         |                     | [:like "Shaquile O'Neal"->"JaVale McGee"@0] | ("Magic")        | [:serve "Vince Carter"->"Grizzlies"@0]      | ("Aron Baynes")       | [:serve "Paul Gasol"->"Bucks"@0]                 | ("Steve Nash")        | [:like "James Harden"->"Russell Westbrook"@0] |
      |                                         |                     | [:like "Shaquile O'Neal"->"Tim Duncan"@0]   | ("Spurs")        | [:serve "Vince Carter"->"Hawks"@0]          | ("Boris Diaw")        | [:serve "Paul Gasol"->"Bulls"@0]                 | ("Marc Gasol")        | [:serve "Chris Paul"->"Clippers"@0]           |
      |                                         |                     | [:serve "Tracy McGrady"->"Magic"@0]         | ("Celtics")      | [:serve "Vince Carter"->"Kings"@0]          | ("Danny Green")       | [:serve "Paul Gasol"->"Grizzlies"@0]             | ("Kyle Anderson")     | [:serve "Chris Paul"->"Hornets"@0]            |
      |                                         |                     | [:serve "Tracy McGrady"->"Raptors"@0]       | ("Heat")         | [:serve "Vince Carter"->"Magic"@0]          | ("LaMarcus Aldridge") | [:serve "Paul Gasol"->"Lakers"@0]                | ("Russell Westbrook") | [:serve "Chris Paul"->"Rockets"@0]            |
      |                                         |                     | [:serve "Tracy McGrady"->"Rockets"@0]       | ("Suns")         | [:serve "Vince Carter"->"Mavericks"@0]      | ("Tiago Splitter")    | [:serve "Paul Gasol"->"Spurs"@0]                 | ("76ers")             | [:serve "Dirk Nowitzki"->"Mavericks"@0]       |
      |                                         |                     | [:serve "Tracy McGrady"->"Spurs"@0]         | ("Lakers")       | [:serve "Vince Carter"->"Nets"@0]           | ("Pistons")           | [:like "Marc Gasol"->"Paul Gasol"@0]             | ("Hornets")           | [:like "Chris Paul"->"LeBron James"@0]        |
      |                                         |                     | [:like "Grant Hill"->"Tracy McGrady"@0]     | ("Cavaliers")    | [:serve "Vince Carter"->"Raptors"@0]        | ("Nets")              | [:like "Paul Gasol"->"Marc Gasol"@0]             | ("Bucks")             | [:serve "Steve Nash"->"Lakers"@0]             |
      |                                         |                     | [:like "Vince Carter"->"Tracy McGrady"@0]   | ("Raptors")      | [:serve "Vince Carter"->"Suns"@0]           | ("Kings")             | [:serve "Jason Kidd"->"Knicks"@0]                | ("Knicks")            | [:serve "Steve Nash"->"Mavericks"@0]          |
      |                                         |                     | [:like "Tracy McGrady"->"Grant Hill"@0]     |                  | [:like "Jason Kidd"->"Vince Carter"@0]      | ("Clippers")          | [:serve "Jason Kidd"->"Mavericks"@0]             | ("Bulls")             | [:serve "Steve Nash"->"Suns"@0]               |
      |                                         |                     | [:like "Tracy McGrady"->"Kobe Bryant"@0]    |                  | [:like "Vince Carter"->"Jason Kidd"@0]      | ("Mavericks")         | [:serve "Jason Kidd"->"Nets"@0]                  | ("Trail Blazers")     | [:serve "Steve Nash"->"Suns"@1]               |
      |                                         |                     | [:like "Tracy McGrady"->"Rudy Gay"@0]       |                  | [:serve "Tim Duncan"->"Spurs"@0]            | ("Hawks")             | [:serve "Jason Kidd"->"Suns"@0]                  | ("Jazz")              | [:serve "LeBron James"->"Cavaliers"@1]        |
      |                                         |                     |                                             |                  | [:teammate "Manu Ginobili"->"Tim Duncan"@0] | ("Warriors")          | [:serve "Jason Kidd"->"Mavericks"@1]             |                       | [:serve "LeBron James"->"Lakers"@0]           |
      |                                         |                     |                                             |                  | [:teammate "Tony Parker"->"Tim Duncan"@0]   | ("Nuggets")           | [:like "Dirk Nowitzki"->"Jason Kidd"@0]          |                       | [:serve "LeBron James"->"Heat"@0]             |
      |                                         |                     |                                             |                  | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("Grizzlies")         | [:like "Steve Nash"->"Jason Kidd"@0]             |                       | [:serve "Marc Gasol"->"Grizzlies"@0]          |
      |                                         |                     |                                             |                  | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Wizards")           | [:like "Jason Kidd"->"Dirk Nowitzki"@0]          |                       | [:serve "Marc Gasol"->"Raptors"@0]            |
      |                                         |                     |                                             |                  | [:like "Danny Green"->"Tim Duncan"@0]       |                       | [:like "Jason Kidd"->"Steve Nash"@0]             |                       | [:serve "Kyle Anderson"->"Grizzlies"@0]       |
      |                                         |                     |                                             |                  | [:like "Dejounte Murray"->"Tim Duncan"@0]   |                       | [:serve "Tony Parker"->"Hornets"@0]              |                       | [:serve "Kyle Anderson"->"Spurs"@0]           |
      |                                         |                     |                                             |                  | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] |                       | [:serve "Tony Parker"->"Spurs"@0]                |                       | [:teammate "Tony Parker"->"Kyle Anderson"@0]  |
      |                                         |                     |                                             |                  | [:like "Manu Ginobili"->"Tim Duncan"@0]     |                       | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |                       | [:serve "LeBron James"->"Cavaliers"@0]        |
      |                                         |                     |                                             |                  | [:like "Marco Belinelli"->"Tim Duncan"@0]   |                       | [:teammate "Tim Duncan"->"Tony Parker"@0]        |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tiago Splitter"->"Tim Duncan"@0]    |                       | [:like "Boris Diaw"->"Tony Parker"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tony Parker"->"Tim Duncan"@0]       |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tim Duncan"->"Tony Parker"@0]       |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Lakers"@0]         |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Mavericks"@0]      |                       | [:serve "Marco Belinelli"->"76ers"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Nuggets"@0]        |                       | [:serve "Marco Belinelli"->"Bulls"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Warriors"@0]       |                       | [:serve "Marco Belinelli"->"Hawks"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Wizards"@0]        |                       | [:serve "Marco Belinelli"->"Hornets"@0]          |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Rudy Gay"->"Grizzlies"@0]          |                       | [:serve "Marco Belinelli"->"Kings"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Rudy Gay"->"Kings"@0]              |                       | [:serve "Marco Belinelli"->"Raptors"@0]          |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Rudy Gay"->"Raptors"@0]            |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Rudy Gay"->"Spurs"@0]              |                       | [:serve "Marco Belinelli"->"Warriors"@0]         |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]   |                       | [:serve "Marco Belinelli"->"Hornets"@1]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Danny Green"->"Marco Belinelli"@0]       |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Marco Belinelli"->"Danny Green"@0]       |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Chris Paul"@0]        |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Danny Green"@0]       |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"James Harden"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Kevin Durant"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Aron Baynes"->"Celtics"@0]              |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Aron Baynes"->"Pistons"@0]              |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Aron Baynes"->"Spurs"@0]                |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Hawks"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Hornets"@0]               |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Jazz"@0]                  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Spurs"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Suns"@0]                  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Cavaliers"@0]            |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Raptors"@0]              |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Spurs"@0]                |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tim Duncan"->"Danny Green"@0]        |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Danny Green"->"LeBron James"@0]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Tiago Splitter"->"76ers"@0]             |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Tiago Splitter"->"Hawks"@0]             |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Tiago Splitter"->"Spurs"@0]             |                       |                                               |
    Then the result should be, in any order, with relax comparison:
      | _vertices      | _edges    |
      | [("Yao Ming")] | <[edge1]> |
      | <[vertex2]>    | <[edge2]> |
      | <[vertex3]>    | <[edge3]> |
      | <[vertex4]>    | <[edge4]> |
      | <[vertex5]>    | <[edge5]> |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 5 steps from hash('Tony Parker') IN teammate OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                        | vertex2               | edge2                                            | vertex3               | edge3                                         | vertex4             | edge4                                         | vertex5                | edge5                                        | vertex6        | edge6                                |
      | [:serve "Tony Parker"->"Hornets"@0]          | ("Tim Duncan")        | [:serve "Tim Duncan"->"Spurs"@0]                 | ("Aron Baynes")       | [:serve "Aron Baynes"->"Celtics"@0]           | ("Yao Ming")        | [:serve "Yao Ming"->"Rockets"@0]              | ("Grant Hill")         | [:serve "Grant Hill"->"Clippers"@0]          | ("Steve Nash") | [:serve "Steve Nash"->"Lakers"@0]    |
      | [:serve "Tony Parker"->"Spurs"@0]            | ("Boris Diaw")        | [:teammate "Manu Ginobili"->"Tim Duncan"@0]      | ("Rudy Gay")          | [:serve "Aron Baynes"->"Pistons"@0]           | ("Ray Allen")       | [:like "Yao Ming"->"Tracy McGrady"@0]         | ("Kristaps Porzingis") | [:serve "Grant Hill"->"Magic"@0]             | ("Paul Gasol") | [:serve "Steve Nash"->"Mavericks"@0] |
      | [:teammate "Manu Ginobili"->"Tony Parker"@0] | ("LaMarcus Aldridge") | [:teammate "Tony Parker"->"Tim Duncan"@0]        | ("Damian Lillard")    | [:serve "Aron Baynes"->"Spurs"@0]             | ("Blake Griffin")   | [:serve "Ray Allen"->"Bucks"@0]               | ("Dirk Nowitzki")      | [:serve "Grant Hill"->"Pistons"@0]           | ("Jason Kidd") | [:serve "Steve Nash"->"Suns"@0]      |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]    | ("Manu Ginobili")     | [:like "Aron Baynes"->"Tim Duncan"@0]            | ("Kevin Durant")      | [:serve "Rudy Gay"->"Grizzlies"@0]            | ("Paul George")     | [:serve "Ray Allen"->"Celtics"@0]             | ("Rajon Rondo")        | [:serve "Grant Hill"->"Suns"@0]              | ("Pelicans")   | [:serve "Steve Nash"->"Suns"@1]      |
      | [:like "Boris Diaw"->"Tony Parker"@0]        | ("Marco Belinelli")   | [:like "Boris Diaw"->"Tim Duncan"@0]             | ("Shaquile O'Neal")   | [:serve "Rudy Gay"->"Kings"@0]                | ("JaVale McGee")    | [:serve "Ray Allen"->"Heat"@0]                | ("Vince Carter")       | [:serve "Kristaps Porzingis"->"Knicks"@0]    | ("Nets")       | [:like "Jason Kidd"->"Steve Nash"@0] |
      | [:like "Dejounte Murray"->"Tony Parker"@0]   | ("Dejounte Murray")   | [:like "Danny Green"->"Tim Duncan"@0]            | ("Tiago Splitter")    | [:serve "Rudy Gay"->"Raptors"@0]              | ("Luka Doncic")     | [:serve "Ray Allen"->"Thunders"@0]            | ("Kobe Bryant")        | [:serve "Kristaps Porzingis"->"Mavericks"@0] |                | [:serve "Paul Gasol"->"Spurs"@0]     |
      | [:like "LaMarcus Aldridge"->"Tony Parker"@0] | ("Hornets")           | [:like "Dejounte Murray"->"Tim Duncan"@0]        | ("Russell Westbrook") | [:serve "Rudy Gay"->"Spurs"@0]                | ("Carmelo Anthony") | [:like "Rajon Rondo"->"Ray Allen"@0]          | ("Wizards")            | [:serve "Dirk Nowitzki"->"Mavericks"@0]      |                | [:like "Steve Nash"->"Jason Kidd"@0] |
      | [:like "Marco Belinelli"->"Tony Parker"@0]   | ("Spurs")             | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]      | ("Danny Green")       | [:like "Tracy McGrady"->"Rudy Gay"@0]         | ("Tracy McGrady")   | [:like "Ray Allen"->"Rajon Rondo"@0]          | ("Pacers")             | [:like "Jason Kidd"->"Dirk Nowitzki"@0]      |                | [:serve "Paul Gasol"->"Lakers"@0]    |
      | [:like "Tim Duncan"->"Tony Parker"@0]        |                       | [:like "Manu Ginobili"->"Tim Duncan"@0]          | ("Kyle Anderson")     | [:serve "Damian Lillard"->"Trail Blazers"@0]  | ("Dwyane Wade")     | [:serve "Blake Griffin"->"Clippers"@0]        | ("Knicks")             | [:like "Steve Nash"->"Dirk Nowitzki"@0]      |                | [:serve "Jason Kidd"->"Knicks"@0]    |
      | [:like "Tony Parker"->"LaMarcus Aldridge"@0] |                       | [:like "Marco Belinelli"->"Tim Duncan"@0]        | ("James Harden")      | [:serve "Kevin Durant"->"Thunders"@0]         | ("Kyrie Irving")    | [:serve "Blake Griffin"->"Pistons"@0]         | ("Bucks")              | [:like "Dirk Nowitzki"->"Jason Kidd"@0]      |                | [:serve "Jason Kidd"->"Mavericks"@0] |
      | [:like "Tony Parker"->"Manu Ginobili"@0]     |                       | [:like "Shaquile O'Neal"->"Tim Duncan"@0]        | ("LeBron James")      | [:serve "Kevin Durant"->"Warriors"@0]         | ("Cavaliers")       | [:serve "Paul George"->"Pacers"@0]            | ("Mavericks")          | [:like "Dirk Nowitzki"->"Steve Nash"@0]      |                | [:serve "Jason Kidd"->"Nets"@0]      |
      | [:like "Tony Parker"->"Tim Duncan"@0]        |                       | [:like "Tiago Splitter"->"Tim Duncan"@0]         | ("Chris Paul")        | [:serve "Shaquile O'Neal"->"Cavaliers"@0]     | ("Celtics")         | [:serve "Paul George"->"Thunders"@0]          | ("Nuggets")            | [:serve "Rajon Rondo"->"Bulls"@0]            |                | [:serve "Jason Kidd"->"Suns"@0]      |
      |                                              |                       | [:like "Tim Duncan"->"Manu Ginobili"@0]          | ("Bulls")             | [:serve "Shaquile O'Neal"->"Celtics"@0]       | ("Pistons")         | [:serve "JaVale McGee"->"Lakers"@0]           |                        | [:serve "Rajon Rondo"->"Celtics"@0]          |                | [:serve "Jason Kidd"->"Mavericks"@1] |
      |                                              |                       | [:serve "Boris Diaw"->"Hawks"@0]                 | ("Jazz")              | [:serve "Shaquile O'Neal"->"Heat"@0]          | ("Grizzlies")       | [:serve "JaVale McGee"->"Mavericks"@0]        |                        | [:serve "Rajon Rondo"->"Kings"@0]            |                | [:serve "Paul Gasol"->"Bucks"@0]     |
      |                                              |                       | [:serve "Boris Diaw"->"Hornets"@0]               | ("Hawks")             | [:serve "Shaquile O'Neal"->"Lakers"@0]        | ("Heat")            | [:serve "JaVale McGee"->"Nuggets"@0]          |                        | [:serve "Rajon Rondo"->"Lakers"@0]           |                | [:serve "Paul Gasol"->"Bulls"@0]     |
      |                                              |                       | [:serve "Boris Diaw"->"Jazz"@0]                  | ("Warriors")          | [:serve "Shaquile O'Neal"->"Magic"@0]         | ("Magic")           | [:serve "JaVale McGee"->"Warriors"@0]         |                        | [:serve "Rajon Rondo"->"Mavericks"@0]        |                | [:serve "Paul Gasol"->"Grizzlies"@0] |
      |                                              |                       | [:serve "Boris Diaw"->"Spurs"@0]                 | ("Suns")              | [:serve "Shaquile O'Neal"->"Suns"@0]          | ("Lakers")          | [:serve "JaVale McGee"->"Wizards"@0]          |                        | [:serve "Rajon Rondo"->"Pelicans"@0]         |                |                                      |
      |                                              |                       | [:serve "Boris Diaw"->"Suns"@0]                  | ("Trail Blazers")     | [:like "Yao Ming"->"Shaquile O'Neal"@0]       | ("Clippers")        | [:serve "Luka Doncic"->"Mavericks"@0]         |                        | [:serve "Vince Carter"->"Grizzlies"@0]       |                |                                      |
      |                                              |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          | ("Kings")             | [:like "Shaquile O'Neal"->"JaVale McGee"@0]   | ("Thunders")        | [:like "Kristaps Porzingis"->"Luka Doncic"@0] |                        | [:serve "Vince Carter"->"Hawks"@0]           |                |                                      |
      |                                              |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0]  | ("Raptors")           | [:serve "Tiago Splitter"->"76ers"@0]          | ("Rockets")         | [:like "Luka Doncic"->"Dirk Nowitzki"@0]      |                        | [:serve "Vince Carter"->"Kings"@0]           |                |                                      |
      |                                              |                       | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  | ("76ers")             | [:serve "Tiago Splitter"->"Hawks"@0]          |                     | [:like "Luka Doncic"->"Kristaps Porzingis"@0] |                        | [:serve "Vince Carter"->"Magic"@0]           |                |                                      |
      |                                              |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |                       | [:serve "Tiago Splitter"->"Spurs"@0]          |                     | [:serve "Carmelo Anthony"->"Knicks"@0]        |                        | [:serve "Vince Carter"->"Mavericks"@0]       |                |                                      |
      |                                              |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  |                       | [:serve "Russell Westbrook"->"Thunders"@0]    |                     | [:serve "Carmelo Anthony"->"Nuggets"@0]       |                        | [:serve "Vince Carter"->"Nets"@0]            |                |                                      |
      |                                              |                       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]        |                       | [:like "James Harden"->"Russell Westbrook"@0] |                     | [:serve "Carmelo Anthony"->"Rockets"@0]       |                        | [:serve "Vince Carter"->"Raptors"@0]         |                |                                      |
      |                                              |                       | [:serve "Manu Ginobili"->"Spurs"@0]              |                       | [:like "Paul George"->"Russell Westbrook"@0]  |                     | [:serve "Carmelo Anthony"->"Thunders"@0]      |                        | [:serve "Vince Carter"->"Suns"@0]            |                |                                      |
      |                                              |                       | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |                       | [:like "Russell Westbrook"->"James Harden"@0] |                     | [:like "Dwyane Wade"->"Carmelo Anthony"@0]    |                        | [:like "Jason Kidd"->"Vince Carter"@0]       |                |                                      |
      |                                              |                       | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |                       | [:like "Russell Westbrook"->"Paul George"@0]  |                     | [:like "Carmelo Anthony"->"Dwyane Wade"@0]    |                        | [:like "Vince Carter"->"Jason Kidd"@0]       |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |                       | [:serve "Danny Green"->"Cavaliers"@0]         |                     | [:serve "Tracy McGrady"->"Magic"@0]           |                        | [:serve "Kobe Bryant"->"Lakers"@0]           |                |                                      |
      |                                              |                       | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |                       | [:serve "Danny Green"->"Raptors"@0]           |                     | [:serve "Tracy McGrady"->"Raptors"@0]         |                        | [:like "Paul Gasol"->"Kobe Bryant"@0]        |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"76ers"@0]            |                       | [:serve "Danny Green"->"Spurs"@0]             |                     | [:serve "Tracy McGrady"->"Rockets"@0]         |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Bulls"@0]            |                       | [:teammate "Tim Duncan"->"Danny Green"@0]     |                     | [:serve "Tracy McGrady"->"Spurs"@0]           |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hawks"@0]            |                       | [:like "Danny Green"->"LeBron James"@0]       |                     | [:like "Grant Hill"->"Tracy McGrady"@0]       |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hornets"@0]          |                       | [:serve "Kyle Anderson"->"Grizzlies"@0]       |                     | [:like "Vince Carter"->"Tracy McGrady"@0]     |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Kings"@0]            |                       | [:serve "Kyle Anderson"->"Spurs"@0]           |                     | [:like "Tracy McGrady"->"Grant Hill"@0]       |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Raptors"@0]          |                       | [:teammate "Tony Parker"->"Kyle Anderson"@0]  |                     | [:like "Tracy McGrady"->"Kobe Bryant"@0]      |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |                       | [:serve "James Harden"->"Rockets"@0]          |                     | [:serve "Dwyane Wade"->"Bulls"@0]             |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Warriors"@0]         |                       | [:serve "James Harden"->"Thunders"@0]         |                     | [:serve "Dwyane Wade"->"Cavaliers"@0]         |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hornets"@1]          |                       | [:like "Luka Doncic"->"James Harden"@0]       |                     | [:serve "Dwyane Wade"->"Heat"@0]              |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |                       | [:serve "LeBron James"->"Cavaliers"@0]        |                     | [:serve "Dwyane Wade"->"Heat"@1]              |                        |                                              |                |                                      |
      |                                              |                       | [:like "Danny Green"->"Marco Belinelli"@0]       |                       | [:serve "LeBron James"->"Heat"@0]             |                     | [:like "Dirk Nowitzki"->"Dwyane Wade"@0]      |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |                       | [:serve "LeBron James"->"Lakers"@0]           |                     | [:serve "Kyrie Irving"->"Cavaliers"@0]        |                        |                                              |                |                                      |
      |                                              |                       | [:like "Marco Belinelli"->"Danny Green"@0]       |                       | [:serve "LeBron James"->"Cavaliers"@1]        |                     | [:serve "Kyrie Irving"->"Celtics"@0]          |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |                       | [:like "Carmelo Anthony"->"LeBron James"@0]   |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Chris Paul"@0]        |                       | [:like "Chris Paul"->"LeBron James"@0]        |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Danny Green"@0]       |                       | [:like "Dwyane Wade"->"LeBron James"@0]       |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"James Harden"@0]      |                       | [:like "Kyrie Irving"->"LeBron James"@0]      |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Kevin Durant"@0]      |                       | [:like "LeBron James"->"Ray Allen"@0]         |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     |                       | [:serve "Chris Paul"->"Clippers"@0]           |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       | [:serve "Chris Paul"->"Hornets"@0]            |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       | [:serve "Chris Paul"->"Rockets"@0]            |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Blake Griffin"->"Chris Paul"@0]       |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Carmelo Anthony"->"Chris Paul"@0]     |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Dwyane Wade"->"Chris Paul"@0]         |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Chris Paul"->"Carmelo Anthony"@0]     |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Chris Paul"->"Dwyane Wade"@0]         |                     |                                               |                        |                                              |                |                                      |
    Then the result should be, in any order, with relax comparison:
      | _vertices         | _edges    |
      | [("Tony Parker")] | <[edge1]> |
      | <[vertex2]>       | <[edge2]> |
      | <[vertex3]>       | <[edge3]> |
      | <[vertex4]>       | <[edge4]> |
      | <[vertex5]>       | <[edge5]> |
      | <[vertex6]>       | <[edge6]> |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 4 steps from hash('Tim Duncan') BOTH like
      """
    Then define some list variables:
      | edge1                                       | vertex2               | edge2                                            | vertex3               | edge3                                         | vertex4             | edge4                                         | vertex5                |
      | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  | ("Kevin Durant")      | [:like "Luka Doncic"->"James Harden"@0]       | ("Tracy McGrady")   | [:like "Grant Hill"->"Tracy McGrady"@0]       | ("Kobe Bryant")        |
      | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Boris Diaw")        | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]        | ("James Harden")      | [:like "Russell Westbrook"->"James Harden"@0] | ("Carmelo Anthony") | [:like "Vince Carter"->"Tracy McGrady"@0]     | ("Dirk Nowitzki")      |
      | [:like "Danny Green"->"Tim Duncan"@0]       | ("Dejounte Murray")   | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     | ("Chris Paul")        | [:like "James Harden"->"Russell Westbrook"@0] | ("Luka Doncic")     | [:like "Tracy McGrady"->"Grant Hill"@0]       | ("Grant Hill")         |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]   | ("Danny Green")       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     | ("Damian Lillard")    | [:like "Blake Griffin"->"Chris Paul"@0]       | ("Blake Griffin")   | [:like "Tracy McGrady"->"Kobe Bryant"@0]      | ("Vince Carter")       |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] | ("Marco Belinelli")   | [:like "Boris Diaw"->"Tony Parker"@0]            | ("Rudy Gay")          | [:like "Carmelo Anthony"->"Chris Paul"@0]     | ("Dwyane Wade")     | [:like "Dwyane Wade"->"Carmelo Anthony"@0]    | ("Rajon Rondo")        |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]     | ("Aron Baynes")       | [:like "Dejounte Murray"->"Chris Paul"@0]        | ("Kyle Anderson")     | [:like "Dwyane Wade"->"Chris Paul"@0]         | ("Kyrie Irving")    | [:like "Carmelo Anthony"->"Dwyane Wade"@0]    | ("Kristaps Porzingis") |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]   | ("Manu Ginobili")     | [:like "Dejounte Murray"->"Danny Green"@0]       | ("LeBron James")      | [:like "Chris Paul"->"Carmelo Anthony"@0]     | ("Ray Allen")       | [:like "Kristaps Porzingis"->"Luka Doncic"@0] |                        |
      | [:like "Shaquile O'Neal"->"Tim Duncan"@0]   | ("Tiago Splitter")    | [:like "Dejounte Murray"->"James Harden"@0]      | ("Russell Westbrook") | [:like "Chris Paul"->"Dwyane Wade"@0]         | ("Paul George")     | [:like "Luka Doncic"->"Dirk Nowitzki"@0]      |                        |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]    | ("Shaquile O'Neal")   | [:like "Dejounte Murray"->"Kevin Durant"@0]      | ("Yao Ming")          | [:like "Chris Paul"->"LeBron James"@0]        |                     | [:like "Luka Doncic"->"Kristaps Porzingis"@0] |                        |
      | [:like "Tony Parker"->"Tim Duncan"@0]       | ("Tony Parker")       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     | ("JaVale McGee")      | [:like "Tracy McGrady"->"Rudy Gay"@0]         |                     | [:like "Dirk Nowitzki"->"Dwyane Wade"@0]      |                        |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       | [:like "Carmelo Anthony"->"LeBron James"@0]   |                     | [:like "Rajon Rondo"->"Ray Allen"@0]          |                        |
      | [:like "Tim Duncan"->"Tony Parker"@0]       |                       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |                       | [:like "Dwyane Wade"->"LeBron James"@0]       |                     | [:like "Ray Allen"->"Rajon Rondo"@0]          |                        |
      |                                             |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |                       | [:like "Kyrie Irving"->"LeBron James"@0]      |                     |                                               |                        |
      |                                             |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       | [:like "LeBron James"->"Ray Allen"@0]         |                     |                                               |                        |
      |                                             |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       |                       | [:like "Paul George"->"Russell Westbrook"@0]  |                     |                                               |                        |
      |                                             |                       | [:like "Marco Belinelli"->"Danny Green"@0]       |                       | [:like "Russell Westbrook"->"Paul George"@0]  |                     |                                               |                        |
      |                                             |                       | [:like "Danny Green"->"LeBron James"@0]          |                       | [:like "Yao Ming"->"Tracy McGrady"@0]         |                     |                                               |                        |
      |                                             |                       | [:like "Danny Green"->"Marco Belinelli"@0]       |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Tony Parker"->"Manu Ginobili"@0]         |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Yao Ming"->"Shaquile O'Neal"@0]          |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Shaquile O'Neal"->"JaVale McGee"@0]      |                       |                                               |                     |                                               |                        |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | <[edge3]> |
      | <[vertex4]>      | <[edge4]> |
      | <[vertex5]>      | []        |

  Scenario: Integer Vid over end
    When executing query:
      """
      GET SUBGRAPH WITH PROP 10000000000000 STEPS FROM hash('Yao Ming') IN teammate OUT serve
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices      | _edges                             |
      | [("Yao Ming")] | [[:serve "Yao Ming"->"Rockets"@0]] |
      | [("Rockets")]  | []                                 |
    When executing query:
      """
      GET SUBGRAPH 10000000000000 STEPS FROM hash('Yao Ming') IN teammate OUT serve
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices      | _edges                             |
      | [("Yao Ming")] | [[:serve "Yao Ming"->"Rockets"@0]] |
      | [("Rockets")]  | []                                 |

  Scenario: Integer Vid many steps without prop
    When executing query:
      """
      GET SUBGRAPH 4 STEPS FROM hash('Yao Ming') IN teammate OUT serve
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices      | _edges                             |
      | [("Yao Ming")] | [[:serve "Yao Ming"->"Rockets"@0]] |
      | [("Rockets")]  | []                                 |
    When executing query:
      """
      GET SUBGRAPH 4 STEPS FROM hash('NOBODY') IN teammate OUT serve
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices    | _edges |
      | [("NOBODY")] | []     |
    When executing query:
      """
      GET SUBGRAPH 4 steps from hash('Yao Ming') IN teammate OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                   | vertex2             | edge2                                       | vertex3          | edge3                                       | vertex4               | edge4                                            | vertex5               | edge5                                         |
      | [:serve "Yao Ming"->"Rockets"@0]        | ("Shaquile O'Neal") | [:serve "Shaquile O'Neal"->"Cavaliers"@0]   | ("Kobe Bryant")  | [:serve "Kobe Bryant"->"Lakers"@0]          | ("Manu Ginobili")     | [:serve "Manu Ginobili"->"Spurs"@0]              | ("Dirk Nowitzki")     | [:like "Dirk Nowitzki"->"Steve Nash"@0]       |
      | [:like "Yao Ming"->"Shaquile O'Neal"@0] | ("Tracy McGrady")   | [:serve "Shaquile O'Neal"->"Celtics"@0]     | ("Grant Hill")   | [:like "Paul Gasol"->"Kobe Bryant"@0]       | ("Paul Gasol")        | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      | ("Kevin Durant")      | [:serve "Kevin Durant"->"Warriors"@0]         |
      | [:like "Yao Ming"->"Tracy McGrady"@0]   | ("Rockets")         | [:serve "Shaquile O'Neal"->"Heat"@0]        | ("Vince Carter") | [:serve "Grant Hill"->"Clippers"@0]         | ("Jason Kidd")        | [:teammate "Tony Parker"->"Manu Ginobili"@0]     | ("Damian Lillard")    | [:serve "Damian Lillard"->"Trail Blazers"@0]  |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Lakers"@0]      | ("Tim Duncan")   | [:serve "Grant Hill"->"Magic"@0]            | ("Tony Parker")       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     | ("James Harden")      | [:serve "James Harden"->"Rockets"@0]          |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Magic"@0]       | ("JaVale McGee") | [:serve "Grant Hill"->"Pistons"@0]          | ("Marco Belinelli")   | [:like "Tiago Splitter"->"Manu Ginobili"@0]      | ("Chris Paul")        | [:like "Steve Nash"->"Dirk Nowitzki"@0]       |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Suns"@0]        | ("Rudy Gay")     | [:serve "Grant Hill"->"Suns"@0]             | ("Dejounte Murray")   | [:like "Tony Parker"->"Manu Ginobili"@0]         | ("LeBron James")      | [:like "Russell Westbrook"->"James Harden"@0] |
      |                                         |                     | [:like "Shaquile O'Neal"->"JaVale McGee"@0] | ("Magic")        | [:serve "Vince Carter"->"Grizzlies"@0]      | ("Aron Baynes")       | [:serve "Paul Gasol"->"Bucks"@0]                 | ("Steve Nash")        | [:like "James Harden"->"Russell Westbrook"@0] |
      |                                         |                     | [:like "Shaquile O'Neal"->"Tim Duncan"@0]   | ("Spurs")        | [:serve "Vince Carter"->"Hawks"@0]          | ("Boris Diaw")        | [:serve "Paul Gasol"->"Bulls"@0]                 | ("Marc Gasol")        | [:serve "Chris Paul"->"Clippers"@0]           |
      |                                         |                     | [:serve "Tracy McGrady"->"Magic"@0]         | ("Celtics")      | [:serve "Vince Carter"->"Kings"@0]          | ("Danny Green")       | [:serve "Paul Gasol"->"Grizzlies"@0]             | ("Kyle Anderson")     | [:serve "Chris Paul"->"Hornets"@0]            |
      |                                         |                     | [:serve "Tracy McGrady"->"Raptors"@0]       | ("Heat")         | [:serve "Vince Carter"->"Magic"@0]          | ("LaMarcus Aldridge") | [:serve "Paul Gasol"->"Lakers"@0]                | ("Russell Westbrook") | [:serve "Chris Paul"->"Rockets"@0]            |
      |                                         |                     | [:serve "Tracy McGrady"->"Rockets"@0]       | ("Suns")         | [:serve "Vince Carter"->"Mavericks"@0]      | ("Tiago Splitter")    | [:serve "Paul Gasol"->"Spurs"@0]                 | ("76ers")             | [:serve "Dirk Nowitzki"->"Mavericks"@0]       |
      |                                         |                     | [:serve "Tracy McGrady"->"Spurs"@0]         | ("Lakers")       | [:serve "Vince Carter"->"Nets"@0]           | ("Pistons")           | [:like "Marc Gasol"->"Paul Gasol"@0]             | ("Hornets")           | [:like "Chris Paul"->"LeBron James"@0]        |
      |                                         |                     | [:like "Grant Hill"->"Tracy McGrady"@0]     | ("Cavaliers")    | [:serve "Vince Carter"->"Raptors"@0]        | ("Nets")              | [:like "Paul Gasol"->"Marc Gasol"@0]             | ("Bucks")             | [:serve "Steve Nash"->"Lakers"@0]             |
      |                                         |                     | [:like "Vince Carter"->"Tracy McGrady"@0]   | ("Raptors")      | [:serve "Vince Carter"->"Suns"@0]           | ("Kings")             | [:serve "Jason Kidd"->"Knicks"@0]                | ("Knicks")            | [:serve "Steve Nash"->"Mavericks"@0]          |
      |                                         |                     | [:like "Tracy McGrady"->"Grant Hill"@0]     |                  | [:like "Jason Kidd"->"Vince Carter"@0]      | ("Clippers")          | [:serve "Jason Kidd"->"Mavericks"@0]             | ("Bulls")             | [:serve "Steve Nash"->"Suns"@0]               |
      |                                         |                     | [:like "Tracy McGrady"->"Kobe Bryant"@0]    |                  | [:like "Vince Carter"->"Jason Kidd"@0]      | ("Mavericks")         | [:serve "Jason Kidd"->"Nets"@0]                  | ("Trail Blazers")     | [:serve "Steve Nash"->"Suns"@1]               |
      |                                         |                     | [:like "Tracy McGrady"->"Rudy Gay"@0]       |                  | [:serve "Tim Duncan"->"Spurs"@0]            | ("Hawks")             | [:serve "Jason Kidd"->"Suns"@0]                  | ("Jazz")              | [:serve "LeBron James"->"Cavaliers"@1]        |
      |                                         |                     |                                             |                  | [:teammate "Manu Ginobili"->"Tim Duncan"@0] | ("Warriors")          | [:serve "Jason Kidd"->"Mavericks"@1]             |                       | [:serve "LeBron James"->"Lakers"@0]           |
      |                                         |                     |                                             |                  | [:teammate "Tony Parker"->"Tim Duncan"@0]   | ("Nuggets")           | [:like "Dirk Nowitzki"->"Jason Kidd"@0]          |                       | [:serve "LeBron James"->"Heat"@0]             |
      |                                         |                     |                                             |                  | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("Grizzlies")         | [:like "Steve Nash"->"Jason Kidd"@0]             |                       | [:serve "Marc Gasol"->"Grizzlies"@0]          |
      |                                         |                     |                                             |                  | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Wizards")           | [:like "Jason Kidd"->"Dirk Nowitzki"@0]          |                       | [:serve "Marc Gasol"->"Raptors"@0]            |
      |                                         |                     |                                             |                  | [:like "Danny Green"->"Tim Duncan"@0]       |                       | [:like "Jason Kidd"->"Steve Nash"@0]             |                       | [:serve "Kyle Anderson"->"Grizzlies"@0]       |
      |                                         |                     |                                             |                  | [:like "Dejounte Murray"->"Tim Duncan"@0]   |                       | [:serve "Tony Parker"->"Hornets"@0]              |                       | [:serve "Kyle Anderson"->"Spurs"@0]           |
      |                                         |                     |                                             |                  | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] |                       | [:serve "Tony Parker"->"Spurs"@0]                |                       | [:teammate "Tony Parker"->"Kyle Anderson"@0]  |
      |                                         |                     |                                             |                  | [:like "Manu Ginobili"->"Tim Duncan"@0]     |                       | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |                       | [:serve "LeBron James"->"Cavaliers"@0]        |
      |                                         |                     |                                             |                  | [:like "Marco Belinelli"->"Tim Duncan"@0]   |                       | [:teammate "Tim Duncan"->"Tony Parker"@0]        |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tiago Splitter"->"Tim Duncan"@0]    |                       | [:like "Boris Diaw"->"Tony Parker"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tony Parker"->"Tim Duncan"@0]       |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tim Duncan"->"Tony Parker"@0]       |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Lakers"@0]         |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Mavericks"@0]      |                       | [:serve "Marco Belinelli"->"76ers"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Nuggets"@0]        |                       | [:serve "Marco Belinelli"->"Bulls"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Warriors"@0]       |                       | [:serve "Marco Belinelli"->"Hawks"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "JaVale McGee"->"Wizards"@0]        |                       | [:serve "Marco Belinelli"->"Hornets"@0]          |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Rudy Gay"->"Grizzlies"@0]          |                       | [:serve "Marco Belinelli"->"Kings"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Rudy Gay"->"Kings"@0]              |                       | [:serve "Marco Belinelli"->"Raptors"@0]          |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Rudy Gay"->"Raptors"@0]            |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Rudy Gay"->"Spurs"@0]              |                       | [:serve "Marco Belinelli"->"Warriors"@0]         |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]   |                       | [:serve "Marco Belinelli"->"Hornets"@1]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Danny Green"->"Marco Belinelli"@0]       |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Marco Belinelli"->"Danny Green"@0]       |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Chris Paul"@0]        |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Danny Green"@0]       |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"James Harden"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Kevin Durant"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Aron Baynes"->"Celtics"@0]              |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Aron Baynes"->"Pistons"@0]              |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Aron Baynes"->"Spurs"@0]                |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Hawks"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Hornets"@0]               |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Jazz"@0]                  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Spurs"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Suns"@0]                  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Cavaliers"@0]            |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Raptors"@0]              |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Spurs"@0]                |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tim Duncan"->"Danny Green"@0]        |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Danny Green"->"LeBron James"@0]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Tiago Splitter"->"76ers"@0]             |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Tiago Splitter"->"Hawks"@0]             |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Tiago Splitter"->"Spurs"@0]             |                       |                                               |
    Then the result should be, in any order, with relax comparison:
      | _vertices      | _edges    |
      | [("Yao Ming")] | <[edge1]> |
      | <[vertex2]>    | <[edge2]> |
      | <[vertex3]>    | <[edge3]> |
      | <[vertex4]>    | <[edge4]> |
      | <[vertex5]>    | <[edge5]> |
    When executing query:
      """
      GET SUBGRAPH 5 steps from hash('Tony Parker') IN teammate OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                        | vertex2               | edge2                                            | vertex3               | edge3                                         | vertex4             | edge4                                         | vertex5                | edge5                                        | vertex6        | edge6                                |
      | [:serve "Tony Parker"->"Hornets"@0]          | ("Tim Duncan")        | [:serve "Tim Duncan"->"Spurs"@0]                 | ("Aron Baynes")       | [:serve "Aron Baynes"->"Celtics"@0]           | ("Yao Ming")        | [:serve "Yao Ming"->"Rockets"@0]              | ("Grant Hill")         | [:serve "Grant Hill"->"Clippers"@0]          | ("Steve Nash") | [:serve "Steve Nash"->"Lakers"@0]    |
      | [:serve "Tony Parker"->"Spurs"@0]            | ("Boris Diaw")        | [:teammate "Manu Ginobili"->"Tim Duncan"@0]      | ("Rudy Gay")          | [:serve "Aron Baynes"->"Pistons"@0]           | ("Ray Allen")       | [:like "Yao Ming"->"Tracy McGrady"@0]         | ("Kristaps Porzingis") | [:serve "Grant Hill"->"Magic"@0]             | ("Paul Gasol") | [:serve "Steve Nash"->"Mavericks"@0] |
      | [:teammate "Manu Ginobili"->"Tony Parker"@0] | ("LaMarcus Aldridge") | [:teammate "Tony Parker"->"Tim Duncan"@0]        | ("Damian Lillard")    | [:serve "Aron Baynes"->"Spurs"@0]             | ("Blake Griffin")   | [:serve "Ray Allen"->"Bucks"@0]               | ("Dirk Nowitzki")      | [:serve "Grant Hill"->"Pistons"@0]           | ("Jason Kidd") | [:serve "Steve Nash"->"Suns"@0]      |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]    | ("Manu Ginobili")     | [:like "Aron Baynes"->"Tim Duncan"@0]            | ("Kevin Durant")      | [:serve "Rudy Gay"->"Grizzlies"@0]            | ("Paul George")     | [:serve "Ray Allen"->"Celtics"@0]             | ("Rajon Rondo")        | [:serve "Grant Hill"->"Suns"@0]              | ("Pelicans")   | [:serve "Steve Nash"->"Suns"@1]      |
      | [:like "Boris Diaw"->"Tony Parker"@0]        | ("Marco Belinelli")   | [:like "Boris Diaw"->"Tim Duncan"@0]             | ("Shaquile O'Neal")   | [:serve "Rudy Gay"->"Kings"@0]                | ("JaVale McGee")    | [:serve "Ray Allen"->"Heat"@0]                | ("Vince Carter")       | [:serve "Kristaps Porzingis"->"Knicks"@0]    | ("Nets")       | [:like "Jason Kidd"->"Steve Nash"@0] |
      | [:like "Dejounte Murray"->"Tony Parker"@0]   | ("Dejounte Murray")   | [:like "Danny Green"->"Tim Duncan"@0]            | ("Tiago Splitter")    | [:serve "Rudy Gay"->"Raptors"@0]              | ("Luka Doncic")     | [:serve "Ray Allen"->"Thunders"@0]            | ("Kobe Bryant")        | [:serve "Kristaps Porzingis"->"Mavericks"@0] |                | [:serve "Paul Gasol"->"Spurs"@0]     |
      | [:like "LaMarcus Aldridge"->"Tony Parker"@0] | ("Hornets")           | [:like "Dejounte Murray"->"Tim Duncan"@0]        | ("Russell Westbrook") | [:serve "Rudy Gay"->"Spurs"@0]                | ("Carmelo Anthony") | [:like "Rajon Rondo"->"Ray Allen"@0]          | ("Wizards")            | [:serve "Dirk Nowitzki"->"Mavericks"@0]      |                | [:like "Steve Nash"->"Jason Kidd"@0] |
      | [:like "Marco Belinelli"->"Tony Parker"@0]   | ("Spurs")             | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]      | ("Danny Green")       | [:like "Tracy McGrady"->"Rudy Gay"@0]         | ("Tracy McGrady")   | [:like "Ray Allen"->"Rajon Rondo"@0]          | ("Pacers")             | [:like "Jason Kidd"->"Dirk Nowitzki"@0]      |                | [:serve "Paul Gasol"->"Lakers"@0]    |
      | [:like "Tim Duncan"->"Tony Parker"@0]        |                       | [:like "Manu Ginobili"->"Tim Duncan"@0]          | ("Kyle Anderson")     | [:serve "Damian Lillard"->"Trail Blazers"@0]  | ("Dwyane Wade")     | [:serve "Blake Griffin"->"Clippers"@0]        | ("Knicks")             | [:like "Steve Nash"->"Dirk Nowitzki"@0]      |                | [:serve "Jason Kidd"->"Knicks"@0]    |
      | [:like "Tony Parker"->"LaMarcus Aldridge"@0] |                       | [:like "Marco Belinelli"->"Tim Duncan"@0]        | ("James Harden")      | [:serve "Kevin Durant"->"Thunders"@0]         | ("Kyrie Irving")    | [:serve "Blake Griffin"->"Pistons"@0]         | ("Bucks")              | [:like "Dirk Nowitzki"->"Jason Kidd"@0]      |                | [:serve "Jason Kidd"->"Mavericks"@0] |
      | [:like "Tony Parker"->"Manu Ginobili"@0]     |                       | [:like "Shaquile O'Neal"->"Tim Duncan"@0]        | ("LeBron James")      | [:serve "Kevin Durant"->"Warriors"@0]         | ("Cavaliers")       | [:serve "Paul George"->"Pacers"@0]            | ("Mavericks")          | [:like "Dirk Nowitzki"->"Steve Nash"@0]      |                | [:serve "Jason Kidd"->"Nets"@0]      |
      | [:like "Tony Parker"->"Tim Duncan"@0]        |                       | [:like "Tiago Splitter"->"Tim Duncan"@0]         | ("Chris Paul")        | [:serve "Shaquile O'Neal"->"Cavaliers"@0]     | ("Celtics")         | [:serve "Paul George"->"Thunders"@0]          | ("Nuggets")            | [:serve "Rajon Rondo"->"Bulls"@0]            |                | [:serve "Jason Kidd"->"Suns"@0]      |
      |                                              |                       | [:like "Tim Duncan"->"Manu Ginobili"@0]          | ("Bulls")             | [:serve "Shaquile O'Neal"->"Celtics"@0]       | ("Pistons")         | [:serve "JaVale McGee"->"Lakers"@0]           |                        | [:serve "Rajon Rondo"->"Celtics"@0]          |                | [:serve "Jason Kidd"->"Mavericks"@1] |
      |                                              |                       | [:serve "Boris Diaw"->"Hawks"@0]                 | ("Jazz")              | [:serve "Shaquile O'Neal"->"Heat"@0]          | ("Grizzlies")       | [:serve "JaVale McGee"->"Mavericks"@0]        |                        | [:serve "Rajon Rondo"->"Kings"@0]            |                | [:serve "Paul Gasol"->"Bucks"@0]     |
      |                                              |                       | [:serve "Boris Diaw"->"Hornets"@0]               | ("Hawks")             | [:serve "Shaquile O'Neal"->"Lakers"@0]        | ("Heat")            | [:serve "JaVale McGee"->"Nuggets"@0]          |                        | [:serve "Rajon Rondo"->"Lakers"@0]           |                | [:serve "Paul Gasol"->"Bulls"@0]     |
      |                                              |                       | [:serve "Boris Diaw"->"Jazz"@0]                  | ("Warriors")          | [:serve "Shaquile O'Neal"->"Magic"@0]         | ("Magic")           | [:serve "JaVale McGee"->"Warriors"@0]         |                        | [:serve "Rajon Rondo"->"Mavericks"@0]        |                | [:serve "Paul Gasol"->"Grizzlies"@0] |
      |                                              |                       | [:serve "Boris Diaw"->"Spurs"@0]                 | ("Suns")              | [:serve "Shaquile O'Neal"->"Suns"@0]          | ("Lakers")          | [:serve "JaVale McGee"->"Wizards"@0]          |                        | [:serve "Rajon Rondo"->"Pelicans"@0]         |                |                                      |
      |                                              |                       | [:serve "Boris Diaw"->"Suns"@0]                  | ("Trail Blazers")     | [:like "Yao Ming"->"Shaquile O'Neal"@0]       | ("Clippers")        | [:serve "Luka Doncic"->"Mavericks"@0]         |                        | [:serve "Vince Carter"->"Grizzlies"@0]       |                |                                      |
      |                                              |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          | ("Kings")             | [:like "Shaquile O'Neal"->"JaVale McGee"@0]   | ("Thunders")        | [:like "Kristaps Porzingis"->"Luka Doncic"@0] |                        | [:serve "Vince Carter"->"Hawks"@0]           |                |                                      |
      |                                              |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0]  | ("Raptors")           | [:serve "Tiago Splitter"->"76ers"@0]          | ("Rockets")         | [:like "Luka Doncic"->"Dirk Nowitzki"@0]      |                        | [:serve "Vince Carter"->"Kings"@0]           |                |                                      |
      |                                              |                       | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  | ("76ers")             | [:serve "Tiago Splitter"->"Hawks"@0]          |                     | [:like "Luka Doncic"->"Kristaps Porzingis"@0] |                        | [:serve "Vince Carter"->"Magic"@0]           |                |                                      |
      |                                              |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |                       | [:serve "Tiago Splitter"->"Spurs"@0]          |                     | [:serve "Carmelo Anthony"->"Knicks"@0]        |                        | [:serve "Vince Carter"->"Mavericks"@0]       |                |                                      |
      |                                              |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  |                       | [:serve "Russell Westbrook"->"Thunders"@0]    |                     | [:serve "Carmelo Anthony"->"Nuggets"@0]       |                        | [:serve "Vince Carter"->"Nets"@0]            |                |                                      |
      |                                              |                       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]        |                       | [:like "James Harden"->"Russell Westbrook"@0] |                     | [:serve "Carmelo Anthony"->"Rockets"@0]       |                        | [:serve "Vince Carter"->"Raptors"@0]         |                |                                      |
      |                                              |                       | [:serve "Manu Ginobili"->"Spurs"@0]              |                       | [:like "Paul George"->"Russell Westbrook"@0]  |                     | [:serve "Carmelo Anthony"->"Thunders"@0]      |                        | [:serve "Vince Carter"->"Suns"@0]            |                |                                      |
      |                                              |                       | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |                       | [:like "Russell Westbrook"->"James Harden"@0] |                     | [:like "Dwyane Wade"->"Carmelo Anthony"@0]    |                        | [:like "Jason Kidd"->"Vince Carter"@0]       |                |                                      |
      |                                              |                       | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |                       | [:like "Russell Westbrook"->"Paul George"@0]  |                     | [:like "Carmelo Anthony"->"Dwyane Wade"@0]    |                        | [:like "Vince Carter"->"Jason Kidd"@0]       |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |                       | [:serve "Danny Green"->"Cavaliers"@0]         |                     | [:serve "Tracy McGrady"->"Magic"@0]           |                        | [:serve "Kobe Bryant"->"Lakers"@0]           |                |                                      |
      |                                              |                       | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |                       | [:serve "Danny Green"->"Raptors"@0]           |                     | [:serve "Tracy McGrady"->"Raptors"@0]         |                        | [:like "Paul Gasol"->"Kobe Bryant"@0]        |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"76ers"@0]            |                       | [:serve "Danny Green"->"Spurs"@0]             |                     | [:serve "Tracy McGrady"->"Rockets"@0]         |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Bulls"@0]            |                       | [:teammate "Tim Duncan"->"Danny Green"@0]     |                     | [:serve "Tracy McGrady"->"Spurs"@0]           |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hawks"@0]            |                       | [:like "Danny Green"->"LeBron James"@0]       |                     | [:like "Grant Hill"->"Tracy McGrady"@0]       |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hornets"@0]          |                       | [:serve "Kyle Anderson"->"Grizzlies"@0]       |                     | [:like "Vince Carter"->"Tracy McGrady"@0]     |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Kings"@0]            |                       | [:serve "Kyle Anderson"->"Spurs"@0]           |                     | [:like "Tracy McGrady"->"Grant Hill"@0]       |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Raptors"@0]          |                       | [:teammate "Tony Parker"->"Kyle Anderson"@0]  |                     | [:like "Tracy McGrady"->"Kobe Bryant"@0]      |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |                       | [:serve "James Harden"->"Rockets"@0]          |                     | [:serve "Dwyane Wade"->"Bulls"@0]             |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Warriors"@0]         |                       | [:serve "James Harden"->"Thunders"@0]         |                     | [:serve "Dwyane Wade"->"Cavaliers"@0]         |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hornets"@1]          |                       | [:like "Luka Doncic"->"James Harden"@0]       |                     | [:serve "Dwyane Wade"->"Heat"@0]              |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |                       | [:serve "LeBron James"->"Cavaliers"@0]        |                     | [:serve "Dwyane Wade"->"Heat"@1]              |                        |                                              |                |                                      |
      |                                              |                       | [:like "Danny Green"->"Marco Belinelli"@0]       |                       | [:serve "LeBron James"->"Heat"@0]             |                     | [:like "Dirk Nowitzki"->"Dwyane Wade"@0]      |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |                       | [:serve "LeBron James"->"Lakers"@0]           |                     | [:serve "Kyrie Irving"->"Cavaliers"@0]        |                        |                                              |                |                                      |
      |                                              |                       | [:like "Marco Belinelli"->"Danny Green"@0]       |                       | [:serve "LeBron James"->"Cavaliers"@1]        |                     | [:serve "Kyrie Irving"->"Celtics"@0]          |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |                       | [:like "Carmelo Anthony"->"LeBron James"@0]   |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Chris Paul"@0]        |                       | [:like "Chris Paul"->"LeBron James"@0]        |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Danny Green"@0]       |                       | [:like "Dwyane Wade"->"LeBron James"@0]       |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"James Harden"@0]      |                       | [:like "Kyrie Irving"->"LeBron James"@0]      |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Kevin Durant"@0]      |                       | [:like "LeBron James"->"Ray Allen"@0]         |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     |                       | [:serve "Chris Paul"->"Clippers"@0]           |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       | [:serve "Chris Paul"->"Hornets"@0]            |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       | [:serve "Chris Paul"->"Rockets"@0]            |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Blake Griffin"->"Chris Paul"@0]       |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Carmelo Anthony"->"Chris Paul"@0]     |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Dwyane Wade"->"Chris Paul"@0]         |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Chris Paul"->"Carmelo Anthony"@0]     |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Chris Paul"->"Dwyane Wade"@0]         |                     |                                               |                        |                                              |                |                                      |
    Then the result should be, in any order, with relax comparison:
      | _vertices         | _edges    |
      | [("Tony Parker")] | <[edge1]> |
      | <[vertex2]>       | <[edge2]> |
      | <[vertex3]>       | <[edge3]> |
      | <[vertex4]>       | <[edge4]> |
      | <[vertex5]>       | <[edge5]> |
      | <[vertex6]>       | <[edge6]> |
    When executing query:
      """
      GET SUBGRAPH 4 steps from hash('Tim Duncan') BOTH like
      """
    Then define some list variables:
      | edge1                                       | vertex2               | edge2                                            | vertex3               | edge3                                         | vertex4             | edge4                                         | vertex5                |
      | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  | ("Kevin Durant")      | [:like "Luka Doncic"->"James Harden"@0]       | ("Tracy McGrady")   | [:like "Grant Hill"->"Tracy McGrady"@0]       | ("Kobe Bryant")        |
      | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Boris Diaw")        | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]        | ("James Harden")      | [:like "Russell Westbrook"->"James Harden"@0] | ("Carmelo Anthony") | [:like "Vince Carter"->"Tracy McGrady"@0]     | ("Dirk Nowitzki")      |
      | [:like "Danny Green"->"Tim Duncan"@0]       | ("Dejounte Murray")   | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     | ("Chris Paul")        | [:like "James Harden"->"Russell Westbrook"@0] | ("Luka Doncic")     | [:like "Tracy McGrady"->"Grant Hill"@0]       | ("Grant Hill")         |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]   | ("Danny Green")       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     | ("Damian Lillard")    | [:like "Blake Griffin"->"Chris Paul"@0]       | ("Blake Griffin")   | [:like "Tracy McGrady"->"Kobe Bryant"@0]      | ("Vince Carter")       |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] | ("Marco Belinelli")   | [:like "Boris Diaw"->"Tony Parker"@0]            | ("Rudy Gay")          | [:like "Carmelo Anthony"->"Chris Paul"@0]     | ("Dwyane Wade")     | [:like "Dwyane Wade"->"Carmelo Anthony"@0]    | ("Rajon Rondo")        |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]     | ("Aron Baynes")       | [:like "Dejounte Murray"->"Chris Paul"@0]        | ("Kyle Anderson")     | [:like "Dwyane Wade"->"Chris Paul"@0]         | ("Kyrie Irving")    | [:like "Carmelo Anthony"->"Dwyane Wade"@0]    | ("Kristaps Porzingis") |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]   | ("Manu Ginobili")     | [:like "Dejounte Murray"->"Danny Green"@0]       | ("LeBron James")      | [:like "Chris Paul"->"Carmelo Anthony"@0]     | ("Ray Allen")       | [:like "Kristaps Porzingis"->"Luka Doncic"@0] |                        |
      | [:like "Shaquile O'Neal"->"Tim Duncan"@0]   | ("Tiago Splitter")    | [:like "Dejounte Murray"->"James Harden"@0]      | ("Russell Westbrook") | [:like "Chris Paul"->"Dwyane Wade"@0]         | ("Paul George")     | [:like "Luka Doncic"->"Dirk Nowitzki"@0]      |                        |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]    | ("Shaquile O'Neal")   | [:like "Dejounte Murray"->"Kevin Durant"@0]      | ("Yao Ming")          | [:like "Chris Paul"->"LeBron James"@0]        |                     | [:like "Luka Doncic"->"Kristaps Porzingis"@0] |                        |
      | [:like "Tony Parker"->"Tim Duncan"@0]       | ("Tony Parker")       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     | ("JaVale McGee")      | [:like "Tracy McGrady"->"Rudy Gay"@0]         |                     | [:like "Dirk Nowitzki"->"Dwyane Wade"@0]      |                        |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       | [:like "Carmelo Anthony"->"LeBron James"@0]   |                     | [:like "Rajon Rondo"->"Ray Allen"@0]          |                        |
      | [:like "Tim Duncan"->"Tony Parker"@0]       |                       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |                       | [:like "Dwyane Wade"->"LeBron James"@0]       |                     | [:like "Ray Allen"->"Rajon Rondo"@0]          |                        |
      |                                             |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |                       | [:like "Kyrie Irving"->"LeBron James"@0]      |                     |                                               |                        |
      |                                             |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       | [:like "LeBron James"->"Ray Allen"@0]         |                     |                                               |                        |
      |                                             |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       |                       | [:like "Paul George"->"Russell Westbrook"@0]  |                     |                                               |                        |
      |                                             |                       | [:like "Marco Belinelli"->"Danny Green"@0]       |                       | [:like "Russell Westbrook"->"Paul George"@0]  |                     |                                               |                        |
      |                                             |                       | [:like "Danny Green"->"LeBron James"@0]          |                       | [:like "Yao Ming"->"Tracy McGrady"@0]         |                     |                                               |                        |
      |                                             |                       | [:like "Danny Green"->"Marco Belinelli"@0]       |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Tony Parker"->"Manu Ginobili"@0]         |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Yao Ming"->"Shaquile O'Neal"@0]          |                       |                                               |                     |                                               |                        |
      |                                             |                       | [:like "Shaquile O'Neal"->"JaVale McGee"@0]      |                       |                                               |                     |                                               |                        |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | <[edge3]> |
      | <[vertex4]>      | <[edge4]> |
      | <[vertex5]>      | []        |

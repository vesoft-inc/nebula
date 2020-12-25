# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Integer Vid subgraph

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: Integer Vid invalid input
    When executing query:
      """
      GET SUBGRAPH FROM $-.id
      """
    Then a SemanticError should be raised at runtime: `$-.id', not exist prop `id'
    When executing query:
      """
      GET SUBGRAPH FROM $a.id
      """
    Then a SemanticError should be raised at runtime: `$a.id', not exist variable `a'
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like YIELD $$.player.name AS id | GET SUBGRAPH FROM $-.id
      """
    Then a SemanticError should be raised at runtime: `$-.id', the srcs should be type of INT64, but was`STRING'
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") OVER like YIELD $$.player.name AS ID; GET SUBGRAPH FROM $a.ID
      """
    Then a SemanticError should be raised at runtime: `$a.ID', the srcs should be type of INT64, but was`STRING'
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") OVER like YIELD like._src AS src; GET SUBGRAPH FROM $b.src
      """
    Then a SemanticError should be raised at runtime: `$b.src', not exist variable `b'
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS id, like._src AS id | GET SUBGRAPH FROM $-.id
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") OVER like YIELD like._dst AS id, like._src AS id; GET SUBGRAPH FROM $a.id
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'

  Scenario: Integer Vid zero step
    When executing query:
      """
      GET SUBGRAPH 0 STEPS FROM hash("Tim Duncan")
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices        |
      | [("Tim Duncan")] |
    When executing query:
      """
      GET SUBGRAPH 0 STEPS FROM hash("Tim Duncan"), hash("Spurs")
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                   |
      | [("Tim Duncan"), ("Spurs")] |
    When executing query:
      """
      GET SUBGRAPH 0 STEPS FROM hash("Tim Duncan"), hash("Tony Parker"), hash("Spurs")
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                                    |
      | [("Tim Duncan"), ("Spurs"), ("Tony Parker")] |
    When executing query:
      """
      GO FROM hash('Tim Duncan') over serve YIELD serve._dst AS id | GET SUBGRAPH 0 STEPS FROM $-.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices   |
      | [("Spurs")] |
    When executing query:
      """
      GO FROM hash('Tim Duncan') over like YIELD like._dst AS id | GET SUBGRAPH 0 STEPS FROM $-.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                            |
      | [("Manu Ginobili"), ("Tony Parker")] |
    When executing query:
      """
      $a = GO FROM hash('Tim Duncan') over serve YIELD serve._dst AS id; GET SUBGRAPH 0 STEPS FROM $a.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices   |
      | [("Spurs")] |
    When executing query:
      """
      $a = GO FROM hash('Tim Duncan') over like YIELD like._dst AS id; GET SUBGRAPH 0 STEPS FROM $a.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                            |
      | [("Manu Ginobili"), ("Tony Parker")] |

  Scenario: Integer Vid subgraph
    When executing query:
      """
      GET SUBGRAPH FROM hash('Tim Duncan')
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
      GET SUBGRAPH 2 STEPS FROM hash('Tim Duncan')
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
      GET SUBGRAPH 2 STEPS FROM hash('Tim Duncan') IN like, serve
      """
    Then define some list variables:
      | edge1                                       | vertex2               | edge2                                           | vertex3             | edge3                                            |
      | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("Manu Ginobili")     | [:like "Dejounte Murray"->"Manu Ginobili"@0]    | ("Damian Lillard")  | [:teammate "Manu Ginobili"->"Tim Duncan"@0]      |
      | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Shaquile O\'Neal")  | [:like "Tiago Splitter"->"Manu Ginobili"@0]     | ("Rudy Gay")        | [:teammate "Tony Parker"->"Tim Duncan"@0]        |
      | [:like "Danny Green"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:like "Tim Duncan"->"Manu Ginobili"@0]         | ("Dejounte Murray") | [:teammate "Tim Duncan"->"Danny Green"@0]        |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]   | ("Marco Belinelli")   | [:like "Tony Parker"->"Manu Ginobili"@0]        | ("Yao Ming")        | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] | ("Danny Green")       | [:like "Yao Ming"->"Shaquile O\'Neal"@0]        | ("Tiago Splitter")  | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]     | ("Tony Parker")       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0] | ("Boris Diaw")      | [:teammate "Tim Duncan"->"Tony Parker"@0]        |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]   |                       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]       |                     | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]  |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]    |                     | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]    |                       | [:like "Danny Green"->"Marco Belinelli"@0]      |                     | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      | [:like "Tony Parker"->"Tim Duncan"@0]       |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]  |                     |                                                  |
      |                                             |                       | [:like "Dejounte Murray"->"Danny Green"@0]      |                     |                                                  |
      |                                             |                       | [:like "Marco Belinelli"->"Danny Green"@0]      |                     |                                                  |
      |                                             |                       | [:like "Boris Diaw"->"Tony Parker"@0]           |                     |                                                  |
      |                                             |                       | [:like "Dejounte Murray"->"Tony Parker"@0]      |                     |                                                  |
      |                                             |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]    |                     |                                                  |
      |                                             |                       | [:like "Marco Belinelli"->"Tony Parker"@0]      |                     |                                                  |
      |                                             |                       | [:like "Tim Duncan"->"Tony Parker"@0]           |                     |                                                  |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | <[edge3]> |

  Scenario: Integer Vid in and out edge
    When executing query:
      """
      GET SUBGRAPH 2 STEPS FROM hash('Tim Duncan') IN like OUT serve
      """
    Then define some list variables:
      | edge1                                       | vertex2               | edge2                                           | vertex3            | edge3                                            |
      | [:serve "Tim Duncan"->"Spurs"@0]            | ("Manu Ginobili")     | [:serve "Manu Ginobili"->"Spurs"@0]             | ("Raptors")        | [:serve "Rudy Gay"->"Raptors"@0]                 |
      | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("Danny Green")       | [:like "Dejounte Murray"->"Manu Ginobili"@0]    | ("Jazz")           | [:serve "Damian Lillard"->"Trail Blazers"@0]     |
      | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Tony Parker")       | [:like "Tiago Splitter"->"Manu Ginobili"@0]     | ("Cavaliers")      | [:serve "Rudy Gay"->"Kings"@0]                   |
      | [:like "Danny Green"->"Tim Duncan"@0]       | ("Aron Baynes")       | [:like "Tim Duncan"->"Manu Ginobili"@0]         | ("Pistons")        | [:serve "Rudy Gay"->"Spurs"@0]                   |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]   | ("Boris Diaw")        | [:like "Tony Parker"->"Manu Ginobili"@0]        | ("Damian Lillard") | [:teammate "Manu Ginobili"->"Tim Duncan"@0]      |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] | ("Shaquile O\'Neal")  | [:serve "Danny Green"->"Cavaliers"@0]           | ("Kings")          | [:teammate "Tony Parker"->"Tim Duncan"@0]        |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]     | ("Dejounte Murray")   | [:serve "Danny Green"->"Raptors"@0]             | ("Hornets")        | [:teammate "Tim Duncan"->"Danny Green"@0]        |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]   | ("LaMarcus Aldridge") | [:serve "Danny Green"->"Spurs"@0]               | ("Spurs")          | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]  | ("Marco Belinelli")   | [:like "Dejounte Murray"->"Danny Green"@0]      | ("Rudy Gay")       | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]    | ("Tiago Splitter")    | [:like "Marco Belinelli"->"Danny Green"@0]      | ("Yao Ming")       | [:teammate "Tim Duncan"->"Tony Parker"@0]        |
      | [:like "Tony Parker"->"Tim Duncan"@0]       |                       | [:serve "Tony Parker"->"Hornets"@0]             | ("Hawks")          | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
      |                                             |                       | [:serve "Tony Parker"->"Spurs"@0]               | ("Heat")           | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      |                                             |                       | [:like "Boris Diaw"->"Tony Parker"@0]           | ("Lakers")         | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      |                                             |                       | [:like "Dejounte Murray"->"Tony Parker"@0]      | ("Celtics")        |                                                  |
      |                                             |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]    | ("Suns")           |                                                  |
      |                                             |                       | [:like "Marco Belinelli"->"Tony Parker"@0]      | ("Magic")          |                                                  |
      |                                             |                       | [:like "Tim Duncan"->"Tony Parker"@0]           | ("Trail Blazers")  |                                                  |
      |                                             |                       | [:serve "Aron Baynes"->"Celtics"@0]             | ("76ers")          |                                                  |
      |                                             |                       | [:serve "Aron Baynes"->"Pistons"@0]             | ("Bulls")          |                                                  |
      |                                             |                       | [:serve "Aron Baynes"->"Spurs"@0]               | ("Warriors")       |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Hawks"@0]                |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Hornets"@0]              |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Jazz"@0]                 |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Spurs"@0]                |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Suns"@0]                 |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O\'Neal"->"Cavaliers"@0]      |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O\'Neal"->"Celtics"@0]        |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O\'Neal"->"Heat"@0]           |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O\'Neal"->"Lakers"@0]         |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O\'Neal"->"Magic"@0]          |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O\'Neal"->"Suns"@0]           |                    |                                                  |
      |                                             |                       | [:like "Yao Ming"->"Shaquile O\'Neal"@0]        |                    |                                                  |
      |                                             |                       | [:serve "Dejounte Murray"->"Spurs"@0]           |                    |                                                  |
      |                                             |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]         |                    |                                                  |
      |                                             |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0] |                    |                                                  |
      |                                             |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0] |                    |                                                  |
      |                                             |                       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]       |                    |                                                  |
      |                                             |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]    |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"76ers"@0]           |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Bulls"@0]           |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Hawks"@0]           |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Hornets"@0]         |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Kings"@0]           |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Raptors"@0]         |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Spurs"@0]           |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Warriors"@0]        |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Hornets"@1]         |                    |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Spurs"@1]           |                    |                                                  |
      |                                             |                       | [:like "Danny Green"->"Marco Belinelli"@0]      |                    |                                                  |
      |                                             |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]  |                    |                                                  |
      |                                             |                       | [:serve "Tiago Splitter"->"76ers"@0]            |                    |                                                  |
      |                                             |                       | [:serve "Tiago Splitter"->"Hawks"@0]            |                    |                                                  |
      |                                             |                       | [:serve "Tiago Splitter"->"Spurs"@0]            |                    |                                                  |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | <[edge3]> |

  Scenario: Integer Vid two steps in and out edge
    When executing query:
      """
      GET SUBGRAPH 2 STEPS FROM hash('Tim Duncan'), hash('James Harden') IN teammate OUT serve
      """
    Then define some list variables:
      | edge1                                       | edge2                                        | edge3                                    |
      | [:serve "Tim Duncan"->"Spurs"@0]            | [:serve "Manu Ginobili"->"Spurs"@0]          | [:like "Manu Ginobili"->"Tim Duncan"@0]  |
      | [:teammate "Manu Ginobili"->"Tim Duncan"@0] | [:teammate "Tim Duncan"->"Manu Ginobili"@0]  | [:like "Tony Parker"->"Tim Duncan"@0]    |
      | [:teammate "Tony Parker"->"Tim Duncan"@0]   | [:teammate "Tony Parker"->"Manu Ginobili"@0] | [:like "Tim Duncan"->"Manu Ginobili"@0]  |
      | [:serve "James Harden"->"Rockets"@0]        | [:serve "Tony Parker"->"Hornets"@0]          | [:like "Tim Duncan"->"Tony Parker"@0]    |
      | [:serve "James Harden"->"Thunders"@0]       | [:serve "Tony Parker"->"Spurs"@0]            | [:like "Tony Parker"->"Manu Ginobili"@0] |
      |                                             | [:teammate "Manu Ginobili"->"Tony Parker"@0] |                                          |
      |                                             | [:teammate "Tim Duncan"->"Tony Parker"@0]    |                                          |
    Then the result should be, in any order, with relax comparison:
      | _vertices                            | _edges    |
      | [("Tim Duncan"), ("James Harden")]   | <[edge1]> |
      | [("Manu Ginobili"), ("Tony Parker")] | <[edge2]> |
      | [("Hornets"), ("Spurs")]             | <[edge3]> |

  Scenario: Integer Vid three steps
    When executing query:
      """
      GET SUBGRAPH 3 STEPS FROM hash('Paul George') OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                        | edge2                                            | edge3                                          | vertex4             | edge4                                        |
      | [:like "Russell Westbrook"->"Paul George"@0] | [:like "Dejounte Murray"->"Russell Westbrook"@0] | [:serve "Dejounte Murray"->"Spurs"@0]          | ("LeBron James")    | [:like "Chris Paul"->"LeBron James"@0]       |
      | [:serve "Paul George"->"Pacers"@0]           | [:like "James Harden"->"Russell Westbrook"@0]    | [:like "Dejounte Murray"->"Chris Paul"@0]      | ("Marco Belinelli") | [:like "Danny Green"->"LeBron James"@0]      |
      | [:serve "Paul George"->"Thunders"@0]         | [:serve "Russell Westbrook"->"Thunders"@0]       | [:like "Dejounte Murray"->"Danny Green"@0]     | ("Danny Green")     | [:like "Danny Green"->"Marco Belinelli"@0]   |
      | [:like "Paul George"->"Russell Westbrook"@0] | [:like "Russell Westbrook"->"James Harden"@0]    | [:like "Dejounte Murray"->"James Harden"@0]    | ("Rockets")         | [:like "Marco Belinelli"->"Danny Green"@0]   |
      |                                              |                                                  | [:like "Dejounte Murray"->"Kevin Durant"@0]    | ("Spurs")           | [:like "Marco Belinelli"->"Tim Duncan"@0]    |
      |                                              |                                                  | [:like "Dejounte Murray"->"Kyle Anderson"@0]   | ("Kevin Durant")    | [:like "Marco Belinelli"->"Tony Parker"@0]   |
      |                                              |                                                  | [:like "Dejounte Murray"->"LeBron James"@0]    | ("Kyle Anderson")   | [:serve "Marco Belinelli"->"Spurs"@0]        |
      |                                              |                                                  | [:like "Dejounte Murray"->"Manu Ginobili"@0]   | ("Tim Duncan")      | [:serve "Marco Belinelli"->"Spurs"@1]        |
      |                                              |                                                  | [:like "Dejounte Murray"->"Marco Belinelli"@0] | ("Tony Parker")     | [:teammate "Tim Duncan"->"Danny Green"@0]    |
      |                                              |                                                  | [:like "Dejounte Murray"->"Tim Duncan"@0]      | ("Chris Paul")      | [:like "Danny Green"->"Tim Duncan"@0]        |
      |                                              |                                                  | [:like "Dejounte Murray"->"Tony Parker"@0]     | ("Luka Doncic")     | [:serve "Danny Green"->"Spurs"@0]            |
      |                                              |                                                  | [:like "Luka Doncic"->"James Harden"@0]        | ("Manu Ginobili")   | [:serve "Chris Paul"->"Rockets"@0]           |
      |                                              |                                                  | [:serve "James Harden"->"Rockets"@0]           | ("Pacers")          | [:serve "Kyle Anderson"->"Spurs"@0]          |
      |                                              |                                                  | [:serve "James Harden"->"Thunders"@0]          | ("Thunders")        | [:serve "Manu Ginobili"->"Spurs"@0]          |
      |                                              |                                                  |                                                |                     | [:serve "Tim Duncan"->"Spurs"@0]             |
      |                                              |                                                  |                                                |                     | [:serve "Tony Parker"->"Spurs"@0]            |
      |                                              |                                                  |                                                |                     | [:serve "Kevin Durant"->"Thunders"@0]        |
      |                                              |                                                  |                                                |                     | [:teammate "Tony Parker"->"Kyle Anderson"@0] |
      |                                              |                                                  |                                                |                     | [:teammate "Manu Ginobili"->"Tim Duncan"@0]  |
      |                                              |                                                  |                                                |                     | [:teammate "Tony Parker"->"Tim Duncan"@0]    |
      |                                              |                                                  |                                                |                     | [:like "Manu Ginobili"->"Tim Duncan"@0]      |
      |                                              |                                                  |                                                |                     | [:like "Tony Parker"->"Tim Duncan"@0]        |
      |                                              |                                                  |                                                |                     | [:like "Tim Duncan"->"Manu Ginobili"@0]      |
      |                                              |                                                  |                                                |                     | [:like "Tim Duncan"->"Tony Parker"@0]        |
      |                                              |                                                  |                                                |                     | [:teammate "Tim Duncan"->"Manu Ginobili"@0]  |
      |                                              |                                                  |                                                |                     | [:teammate "Tim Duncan"->"Tony Parker"@0]    |
      |                                              |                                                  |                                                |                     | [:teammate "Manu Ginobili"->"Tony Parker"@0] |
      |                                              |                                                  |                                                |                     | [:like "Tony Parker"->"Manu Ginobili"@0]     |
      |                                              |                                                  |                                                |                     | [:teammate "Tony Parker"->"Manu Ginobili"@0] |
    Then the result should be, in any order, with relax comparison:
      | _vertices                               | _edges    |
      | [("Paul George")]                       | <[edge1]> |
      | [("Russell Westbrook")]                 | <[edge2]> |
      | [("Dejounte Murray"), ("James Harden")] | <[edge3]> |
      | <[vertex4]>                             | <[edge4]> |

  Scenario: Integer Vid bidirect edge
    When executing query:
      """
      GET SUBGRAPH FROM hash('Tony Parker') BOTH like
      """
    Then define some list variables:
      | edge1                                        | vertex2               | edge2                                            |
      | [:like "Boris Diaw"->"Tony Parker"@0]        | ("Dejounte Murray")   | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |
      | [:like "Dejounte Murray"->"Tony Parker"@0]   | ("LaMarcus Aldridge") | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |
      | [:like "LaMarcus Aldridge"->"Tony Parker"@0] | ("Marco Belinelli")   | [:like "Dejounte Murray"->"Tim Duncan"@0]        |
      | [:like "Marco Belinelli"->"Tony Parker"@0]   | ("Boris Diaw")        | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  |
      | [:like "Tim Duncan"->"Tony Parker"@0]        | ("Tim Duncan")        | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      | [:like "Tony Parker"->"LaMarcus Aldridge"@0] | ("Manu Ginobili")     | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]      |
      | [:like "Tony Parker"->"Manu Ginobili"@0]     |                       | [:like "Marco Belinelli"->"Tim Duncan"@0]        |
      | [:like "Tony Parker"->"Tim Duncan"@0]        |                       | [:like "Boris Diaw"->"Tim Duncan"@0]             |
      |                                              |                       | [:teammate "Manu Ginobili"->"Tim Duncan"@0]      |
      |                                              |                       | [:teammate "Tony Parker"->"Tim Duncan"@0]        |
      |                                              |                       | [:like "Manu Ginobili"->"Tim Duncan"@0]          |
      |                                              |                       | [:like "Tim Duncan"->"Manu Ginobili"@0]          |
      |                                              |                       | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |
      |                                              |                       | [:teammate "Tim Duncan"->"Tony Parker"@0]        |
      |                                              |                       | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      |                                              |                       | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
    Then the result should be, in any order, with relax comparison:
      | _vertices         | _edges    |
      | [("Tony Parker")] | <[edge1]> |
      | <[vertex2]>       | <[edge2]> |

  Scenario: Integer Vid pipe
    When executing query:
      """
      GO FROM hash('Tim Duncan') over serve YIELD serve._src AS id | GET SUBGRAPH FROM $-.id
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
      GET SUBGRAPH FROM $a.id
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

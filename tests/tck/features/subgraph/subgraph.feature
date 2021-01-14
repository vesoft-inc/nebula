# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: subgraph

  Background:
    Given a graph with space named "nba"

  Scenario: invalid input
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
      GO FROM "Tim Duncan" OVER like YIELD $$.player.age AS id | GET SUBGRAPH FROM $-.id
      """
    Then a SemanticError should be raised at runtime: `$-.id', the srcs should be type of FIXED_STRING, but was`INT'
    When executing query:
      """
      $a = GO FROM "Tim Duncan" OVER like YIELD $$.player.age AS ID; GET SUBGRAPH FROM $a.ID
      """
    Then a SemanticError should be raised at runtime: `$a.ID', the srcs should be type of FIXED_STRING, but was`INT'
    When executing query:
      """
      $a = GO FROM "Tim Duncan" OVER like YIELD like._src AS src; GET SUBGRAPH FROM $b.src
      """
    Then a SemanticError should be raised at runtime: `$b.src', not exist variable `b'
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS id, like._src AS id | GET SUBGRAPH FROM $-.id
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'
    When executing query:
      """
      $a = GO FROM "Tim Duncan" OVER like YIELD like._dst AS id, like._src AS id; GET SUBGRAPH FROM $a.id
      """
    Then a SemanticError should be raised at runtime: Duplicate Column Name : `id'

  Scenario: zero step
    When executing query:
      """
      GET SUBGRAPH 0 STEPS FROM "Tim Duncan"
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices        |
      | [("Tim Duncan")] |
    When executing query:
      """
      GET SUBGRAPH 0 STEPS FROM "Tim Duncan", "Spurs"
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                   |
      | [("Tim Duncan"), ("Spurs")] |
    When executing query:
      """
      GET SUBGRAPH 0 STEPS FROM "Tim Duncan", "Tony Parker", "Spurs"
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                                    |
      | [("Tim Duncan"), ("Spurs"), ("Tony Parker")] |
    When executing query:
      """
      GO FROM 'Tim Duncan' over serve YIELD serve._dst AS id | GET SUBGRAPH 0 STEPS FROM $-.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices   |
      | [("Spurs")] |
    When executing query:
      """
      GO FROM 'Tim Duncan' over like YIELD like._dst AS id | GET SUBGRAPH 0 STEPS FROM $-.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                            |
      | [("Manu Ginobili"), ("Tony Parker")] |
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' over serve YIELD serve._dst AS id; GET SUBGRAPH 0 STEPS FROM $a.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices   |
      | [("Spurs")] |
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' over like YIELD like._dst AS id; GET SUBGRAPH 0 STEPS FROM $a.id
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices                            |
      | [("Manu Ginobili"), ("Tony Parker")] |

  Scenario: subgraph
    When executing query:
      """
      GET SUBGRAPH FROM 'Tim Duncan'
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

  Scenario: two steps
    When executing query:
      """
      GET SUBGRAPH 2 STEPS FROM 'Tim Duncan'
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

  Scenario: in edge
    When executing query:
      """
      GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' IN like, serve
      """
    Then define some list variables:
      | edge1                                       | vertex2               | edge2                                           | vertex3            | edge3                                            |
      | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("Manu Ginobili")     | [:like "Dejounte Murray"->"Manu Ginobili"@0]    | ("Damian Lillard") | [:teammate "Manu Ginobili"->"Tim Duncan"@0]      |
      | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Shaquile O\'Neal")  | [:like "Tiago Splitter"->"Manu Ginobili"@0]     | ("Rudy Gay")       | [:teammate "Tony Parker"->"Tim Duncan"@0]        |
      | [:like "Danny Green"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:like "Tim Duncan"->"Manu Ginobili"@0]         | ("Yao Ming")       | [:teammate "Tim Duncan"->"Danny Green"@0]        |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]   | ("Marco Belinelli")   | [:like "Tony Parker"->"Manu Ginobili"@0]        |                    | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] | ("Danny Green")       | [:like "Yao Ming"->"Shaquile O\'Neal"@0]        |                    | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]     | ("Tony Parker")       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0] |                    | [:teammate "Tim Duncan"->"Tony Parker"@0]        |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]   | ("Boris Diaw")        | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]       |                    | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]  | ("Dejounte Murray")   | [:like "Tony Parker"->"LaMarcus Aldridge"@0]    |                    | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]    | ("Aron Baynes")       | [:like "Danny Green"->"Marco Belinelli"@0]      |                    | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      | [:like "Tony Parker"->"Tim Duncan"@0]       | ("Tiago Splitter")    | [:like "Dejounte Murray"->"Marco Belinelli"@0]  |                    |                                                  |
      |                                             |                       | [:like "Dejounte Murray"->"Danny Green"@0]      |                    |                                                  |
      |                                             |                       | [:like "Marco Belinelli"->"Danny Green"@0]      |                    |                                                  |
      |                                             |                       | [:like "Boris Diaw"->"Tony Parker"@0]           |                    |                                                  |
      |                                             |                       | [:like "Dejounte Murray"->"Tony Parker"@0]      |                    |                                                  |
      |                                             |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]    |                    |                                                  |
      |                                             |                       | [:like "Marco Belinelli"->"Tony Parker"@0]      |                    |                                                  |
      |                                             |                       | [:like "Tim Duncan"->"Tony Parker"@0]           |                    |                                                  |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | <[edge3]> |

  Scenario: in and out edge
    When executing query:
      """
      GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' IN like OUT serve
      """
    Then define some list variables:
      | edge1                                       | vertex2               | edge2                                           | vertex3            | edge3                                            |
      | [:serve "Tim Duncan"->"Spurs"@0]            | ("LaMarcus Aldridge") | [:serve "LaMarcus Aldridge"->"Spurs"@0]         | ("Pistons")        | [:teammate "Tim Duncan"->"Danny Green"@0]        |
      | [:like "Aron Baynes"->"Tim Duncan"@0]       | ("Danny Green")       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0] | ("Bulls")          | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      | [:like "Boris Diaw"->"Tim Duncan"@0]        | ("Marco Belinelli")   | [:like "Damian Lillard"->"LaMarcus Aldridge"@0] | ("Lakers")         | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  |
      | [:like "Danny Green"->"Tim Duncan"@0]       | ("Boris Diaw")        | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]       | ("Magic")          | [:serve "Damian Lillard"->"Trail Blazers"@0]     |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]   | ("Dejounte Murray")   | [:like "Tony Parker"->"LaMarcus Aldridge"@0]    | ("Hawks")          | [:serve "Rudy Gay"->"Kings"@0]                   |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] | ("Aron Baynes")       | [:serve "Danny Green"->"Cavaliers"@0]           | ("Warriors")       | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]     | ("Manu Ginobili")     | [:serve "Danny Green"->"Raptors"@0]             | ("Suns")           | [:teammate "Tony Parker"->"Tim Duncan"@0]        |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]   | ("Tiago Splitter")    | [:serve "Danny Green"->"Spurs"@0]               | ("Jazz")           | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
      | [:like "Shaquile O'Neal"->"Tim Duncan"@0]   | ("Shaquile O'Neal")   | [:like "Dejounte Murray"->"Danny Green"@0]      | ("76ers")          | [:teammate "Tim Duncan"->"Tony Parker"@0]        |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]    | ("Tony Parker")       | [:like "Marco Belinelli"->"Danny Green"@0]      | ("Trail Blazers")  | [:serve "Rudy Gay"->"Spurs"@0]                   |
      | [:like "Tony Parker"->"Tim Duncan"@0]       | ("Spurs")             | [:serve "Marco Belinelli"->"76ers"@0]           | ("Celtics")        | [:serve "Rudy Gay"->"Raptors"@0]                 |
      |                                             |                       | [:serve "Marco Belinelli"->"Bulls"@0]           | ("Kings")          | [:teammate "Manu Ginobili"->"Tim Duncan"@0]      |
      |                                             |                       | [:serve "Marco Belinelli"->"Hawks"@0]           | ("Yao Ming")       | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |
      |                                             |                       | [:serve "Marco Belinelli"->"Hornets"@0]         | ("Heat")           |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Kings"@0]           | ("Hornets")        |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Raptors"@0]         | ("Cavaliers")      |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Spurs"@0]           | ("Rudy Gay")       |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Warriors"@0]        | ("Raptors")        |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Hornets"@1]         | ("Damian Lillard") |                                                  |
      |                                             |                       | [:serve "Marco Belinelli"->"Spurs"@1]           |                    |                                                  |
      |                                             |                       | [:like "Danny Green"->"Marco Belinelli"@0]      |                    |                                                  |
      |                                             |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]  |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Hawks"@0]                |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Hornets"@0]              |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Jazz"@0]                 |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Spurs"@0]                |                    |                                                  |
      |                                             |                       | [:serve "Boris Diaw"->"Suns"@0]                 |                    |                                                  |
      |                                             |                       | [:serve "Dejounte Murray"->"Spurs"@0]           |                    |                                                  |
      |                                             |                       | [:serve "Aron Baynes"->"Celtics"@0]             |                    |                                                  |
      |                                             |                       | [:serve "Aron Baynes"->"Pistons"@0]             |                    |                                                  |
      |                                             |                       | [:serve "Aron Baynes"->"Spurs"@0]               |                    |                                                  |
      |                                             |                       | [:serve "Manu Ginobili"->"Spurs"@0]             |                    |                                                  |
      |                                             |                       | [:like "Dejounte Murray"->"Manu Ginobili"@0]    |                    |                                                  |
      |                                             |                       | [:like "Tiago Splitter"->"Manu Ginobili"@0]     |                    |                                                  |
      |                                             |                       | [:like "Tim Duncan"->"Manu Ginobili"@0]         |                    |                                                  |
      |                                             |                       | [:like "Tony Parker"->"Manu Ginobili"@0]        |                    |                                                  |
      |                                             |                       | [:serve "Tiago Splitter"->"76ers"@0]            |                    |                                                  |
      |                                             |                       | [:serve "Tiago Splitter"->"Hawks"@0]            |                    |                                                  |
      |                                             |                       | [:serve "Tiago Splitter"->"Spurs"@0]            |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Cavaliers"@0]       |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Celtics"@0]         |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Heat"@0]            |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Lakers"@0]          |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Magic"@0]           |                    |                                                  |
      |                                             |                       | [:serve "Shaquile O'Neal"->"Suns"@0]            |                    |                                                  |
      |                                             |                       | [:like "Yao Ming"->"Shaquile O'Neal"@0]         |                    |                                                  |
      |                                             |                       | [:serve "Tony Parker"->"Hornets"@0]             |                    |                                                  |
      |                                             |                       | [:serve "Tony Parker"->"Spurs"@0]               |                    |                                                  |
      |                                             |                       | [:like "Boris Diaw"->"Tony Parker"@0]           |                    |                                                  |
      |                                             |                       | [:like "Dejounte Murray"->"Tony Parker"@0]      |                    |                                                  |
      |                                             |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]    |                    |                                                  |
      |                                             |                       | [:like "Marco Belinelli"->"Tony Parker"@0]      |                    |                                                  |
      |                                             |                       | [:like "Tim Duncan"->"Tony Parker"@0]           |                    |                                                  |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |
      | <[vertex3]>      | <[edge3]> |

  Scenario: two steps in and out edge
    When executing query:
      """
      GET SUBGRAPH 2 STEPS FROM 'Tim Duncan', 'James Harden' IN teammate OUT serve
      """
    Then define some list variables:
      | vertex1          | edge1                                       | vertex2           | edge2                                        | vertex3     | edge3                                    |
      | ("Tim Duncan")   | [:serve "Tim Duncan"->"Spurs"@0]            | ("Manu Ginobili") | [:serve "Manu Ginobili"->"Spurs"@0]          | ("Hornets") | [:like "Tim Duncan"->"Manu Ginobili"@0]  |
      | ("James Harden") | [:teammate "Manu Ginobili"->"Tim Duncan"@0] | ("Tony Parker")   | [:teammate "Tim Duncan"->"Manu Ginobili"@0]  |             | [:like "Tim Duncan"->"Tony Parker"@0]    |
      |                  | [:teammate "Tony Parker"->"Tim Duncan"@0]   | ("Spurs")         | [:teammate "Tony Parker"->"Manu Ginobili"@0] |             | [:like "Manu Ginobili"->"Tim Duncan"@0]  |
      |                  | [:serve "James Harden"->"Rockets"@0]        | ("Rockets")       | [:serve "Tony Parker"->"Hornets"@0]          |             | [:like "Tony Parker"->"Manu Ginobili"@0] |
      |                  | [:serve "James Harden"->"Thunders"@0]       | ("Thunders")      | [:serve "Tony Parker"->"Spurs"@0]            |             | [:like "Tony Parker"->"Tim Duncan"@0]    |
      |                  |                                             |                   | [:teammate "Manu Ginobili"->"Tony Parker"@0] |             |                                          |
      |                  |                                             |                   | [:teammate "Tim Duncan"->"Tony Parker"@0]    |             |                                          |
    Then the result should be, in any order, with relax comparison:
      | _vertices   | _edges    |
      | <[vertex1]> | <[edge1]> |
      | <[vertex2]> | <[edge2]> |
      | <[vertex3]> | <[edge3]> |

  Scenario: three steps
    When executing query:
      """
      GET SUBGRAPH 3 STEPS FROM 'Paul George' OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                        | edge2                                            | edge3                                          | vertex4             | edge4                                        |
      | [:like "Russell Westbrook"->"Paul George"@0] | [:like "Dejounte Murray"->"Russell Westbrook"@0] | [:like "Dejounte Murray"->"James Harden"@0]    | ("Tim Duncan")      | [:teammate "Tony Parker"->"Tim Duncan"@0]    |
      | [:serve "Paul George"->"Pacers"@0]           | [:like "James Harden"->"Russell Westbrook"@0]    | [:like "Luka Doncic"->"James Harden"@0]        | ("LeBron James")    | [:teammate "Manu Ginobili"->"Tim Duncan"@0]  |
      | [:serve "Paul George"->"Thunders"@0]         | [:serve "Russell Westbrook"->"Thunders"@0]       | [:serve "James Harden"->"Rockets"@0]           | ("Kyle Anderson")   | [:like "Chris Paul"->"LeBron James"@0]       |
      | [:like "Paul George"->"Russell Westbrook"@0] | [:like "Russell Westbrook"->"James Harden"@0]    | [:serve "James Harden"->"Thunders"@0]          | ("Luka Doncic")     | [:like "Danny Green"->"LeBron James"@0]      |
      |                                              |                                                  | [:serve "Dejounte Murray"->"Spurs"@0]          | ("Danny Green")     | [:teammate "Tim Duncan"->"Tony Parker"@0]    |
      |                                              |                                                  | [:like "Dejounte Murray"->"Chris Paul"@0]      | ("Tony Parker")     | [:teammate "Tim Duncan"->"Manu Ginobili"@0]  |
      |                                              |                                                  | [:like "Dejounte Murray"->"Danny Green"@0]     | ("Chris Paul")      | [:teammate "Tim Duncan"->"Danny Green"@0]    |
      |                                              |                                                  | [:like "Dejounte Murray"->"Kevin Durant"@0]    | ("Rockets")         | [:serve "Tim Duncan"->"Spurs"@0]             |
      |                                              |                                                  | [:like "Dejounte Murray"->"Kyle Anderson"@0]   | ("Kevin Durant")    | [:like "Tony Parker"->"Tim Duncan"@0]        |
      |                                              |                                                  | [:like "Dejounte Murray"->"LeBron James"@0]    | ("Marco Belinelli") | [:like "Marco Belinelli"->"Tim Duncan"@0]    |
      |                                              |                                                  | [:like "Dejounte Murray"->"Manu Ginobili"@0]   | ("Spurs")           | [:like "Manu Ginobili"->"Tim Duncan"@0]      |
      |                                              |                                                  | [:like "Dejounte Murray"->"Marco Belinelli"@0] | ("Manu Ginobili")   | [:serve "Kyle Anderson"->"Spurs"@0]          |
      |                                              |                                                  | [:like "Dejounte Murray"->"Tim Duncan"@0]      |                     | [:teammate "Tony Parker"->"Kyle Anderson"@0] |
      |                                              |                                                  | [:like "Dejounte Murray"->"Tony Parker"@0]     |                     | [:like "Danny Green"->"Tim Duncan"@0]        |
      |                                              |                                                  |                                                |                     | [:like "Tim Duncan"->"Tony Parker"@0]        |
      |                                              |                                                  |                                                |                     | [:like "Tim Duncan"->"Manu Ginobili"@0]      |
      |                                              |                                                  |                                                |                     | [:like "Danny Green"->"Marco Belinelli"@0]   |
      |                                              |                                                  |                                                |                     | [:like "Marco Belinelli"->"Danny Green"@0]   |
      |                                              |                                                  |                                                |                     | [:teammate "Manu Ginobili"->"Tony Parker"@0] |
      |                                              |                                                  |                                                |                     | [:serve "Danny Green"->"Spurs"@0]            |
      |                                              |                                                  |                                                |                     | [:teammate "Tony Parker"->"Manu Ginobili"@0] |
      |                                              |                                                  |                                                |                     | [:serve "Tony Parker"->"Spurs"@0]            |
      |                                              |                                                  |                                                |                     | [:like "Marco Belinelli"->"Tony Parker"@0]   |
      |                                              |                                                  |                                                |                     | [:serve "Chris Paul"->"Rockets"@0]           |
      |                                              |                                                  |                                                |                     | [:like "Tony Parker"->"Manu Ginobili"@0]     |
      |                                              |                                                  |                                                |                     | [:serve "Kevin Durant"->"Thunders"@0]        |
      |                                              |                                                  |                                                |                     | [:serve "Marco Belinelli"->"Spurs"@1]        |
      |                                              |                                                  |                                                |                     | [:serve "Marco Belinelli"->"Spurs"@0]        |
      |                                              |                                                  |                                                |                     | [:serve "Manu Ginobili"->"Spurs"@0]          |
    Then the result should be, in any order, with relax comparison:
      | _vertices                                         | _edges    |
      | [("Paul George")]                                 | <[edge1]> |
      | [("Russell Westbrook"), ("Pacers"), ("Thunders")] | <[edge2]> |
      | [("Dejounte Murray"), ("James Harden")]           | <[edge3]> |
      | <[vertex4]>                                       | <[edge4]> |

  Scenario: bidirect edge
    When executing query:
      """
      GET SUBGRAPH FROM 'Tony Parker' BOTH like
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

  Scenario: pipe
    When executing query:
      """
      GO FROM 'Tim Duncan' over serve YIELD serve._src AS id | GET SUBGRAPH FROM $-.id
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

  Scenario: var
    When executing query:
      """
      $a = GO FROM 'Tim Duncan' over serve YIELD serve._src AS id;
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

  Scenario: many steps
    When executing query:
      """
      GET SUBGRAPH 4 STEPS FROM 'Yao Ming' IN teammate OUT serve
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices      | _edges                             |
      | [("Yao Ming")] | [[:serve "Yao Ming"->"Rockets"@0]] |
      | [("Rockets")]  | []                                 |
      | []             | []                                 |
      | []             | []                                 |
      | []             | []                                 |
    When executing query:
      """
      GET SUBGRAPH 4 STEPS FROM 'NOBODY' IN teammate OUT serve
      """
    Then the result should be, in any order, with relax comparison:
      | _vertices | _edges |
      | []        | []     |
      | []        | []     |
      | []        | []     |
      | []        | []     |
      | []        | []     |
    When executing query:
      """
      Get Subgraph 4 steps from 'Yao Ming' IN teammate OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                   | vertex2             | edge2                                       | vertex3          | edge3                                       | vertex4               | edge4                                            | vertex5               | edge5                                         |
      | [:serve "Yao Ming"->"Rockets"@0]        | ("Shaquile O'Neal") | [:serve "Shaquile O'Neal"->"Cavaliers"@0]   | ("JaVale McGee") | [:serve "JaVale McGee"->"Lakers"@0]         | ("Aron Baynes")       | [:serve "Aron Baynes"->"Celtics"@0]              | ("Damian Lillard")    | [:serve "Damian Lillard"->"Trail Blazers"@0]  |
      | [:like "Yao Ming"->"Shaquile O'Neal"@0] | ("Tracy McGrady")   | [:serve "Shaquile O'Neal"->"Celtics"@0]     | ("Grant Hill")   | [:serve "JaVale McGee"->"Mavericks"@0]      | ("Marco Belinelli")   | [:serve "Aron Baynes"->"Pistons"@0]              | ("LeBron James")      | [:serve "Kyle Anderson"->"Spurs"@0]           |
      | [:like "Yao Ming"->"Tracy McGrady"@0]   | ("Rockets")         | [:serve "Shaquile O'Neal"->"Heat"@0]        | ("Rudy Gay")     | [:serve "JaVale McGee"->"Nuggets"@0]        | ("Tony Parker")       | [:serve "Aron Baynes"->"Spurs"@0]                | ("Dirk Nowitzki")     | [:serve "Chris Paul"->"Rockets"@0]            |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Lakers"@0]      | ("Vince Carter") | [:serve "JaVale McGee"->"Warriors"@0]       | ("Tiago Splitter")    | [:serve "Marco Belinelli"->"76ers"@0]            | ("Steve Nash")        | [:serve "James Harden"->"Rockets"@0]          |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Magic"@0]       | ("Tim Duncan")   | [:serve "JaVale McGee"->"Wizards"@0]        | ("Paul Gasol")        | [:serve "Marco Belinelli"->"Bulls"@0]            | ("Trail Blazers")     | [:serve "Dirk Nowitzki"->"Mavericks"@0]       |
      |                                         |                     | [:serve "Shaquile O'Neal"->"Suns"@0]        | ("Kobe Bryant")  | [:serve "Grant Hill"->"Clippers"@0]         | ("Manu Ginobili")     | [:serve "Marco Belinelli"->"Hawks"@0]            | ("Jazz")              | [:serve "Steve Nash"->"Mavericks"@0]          |
      |                                         |                     | [:like "Shaquile O'Neal"->"JaVale McGee"@0] | ("Cavaliers")    | [:serve "Grant Hill"->"Magic"@0]            | ("Danny Green")       | [:serve "Marco Belinelli"->"Hornets"@0]          | ("Russell Westbrook") | [:like "Chris Paul"->"LeBron James"@0]        |
      |                                         |                     | [:like "Shaquile O'Neal"->"Tim Duncan"@0]   | ("Raptors")      | [:serve "Grant Hill"->"Pistons"@0]          | ("LaMarcus Aldridge") | [:serve "Marco Belinelli"->"Kings"@0]            | ("Kyle Anderson")     | [:serve "LeBron James"->"Cavaliers"@0]        |
      |                                         |                     | [:serve "Tracy McGrady"->"Magic"@0]         | ("Celtics")      | [:serve "Grant Hill"->"Suns"@0]             | ("Boris Diaw")        | [:serve "Marco Belinelli"->"Raptors"@0]          | ("Knicks")            | [:serve "LeBron James"->"Heat"@0]             |
      |                                         |                     | [:serve "Tracy McGrady"->"Raptors"@0]       | ("Heat")         | [:serve "Rudy Gay"->"Grizzlies"@0]          | ("Dejounte Murray")   | [:serve "Marco Belinelli"->"Spurs"@0]            | ("Bulls")             | [:serve "LeBron James"->"Lakers"@0]           |
      |                                         |                     | [:serve "Tracy McGrady"->"Rockets"@0]       | ("Suns")         | [:serve "Rudy Gay"->"Kings"@0]              | ("Jason Kidd")        | [:serve "Marco Belinelli"->"Warriors"@0]         | ("Hornets")           | [:serve "LeBron James"->"Cavaliers"@1]        |
      |                                         |                     | [:serve "Tracy McGrady"->"Spurs"@0]         | ("Magic")        | [:serve "Rudy Gay"->"Raptors"@0]            | ("Kings")             | [:serve "Marco Belinelli"->"Hornets"@1]          | ("Marc Gasol")        | [:like "Dirk Nowitzki"->"Steve Nash"@0]       |
      |                                         |                     | [:like "Grant Hill"->"Tracy McGrady"@0]     | ("Spurs")        | [:serve "Rudy Gay"->"Spurs"@0]              | ("Wizards")           | [:serve "Marco Belinelli"->"Spurs"@1]            | ("76ers")             | [:like "Steve Nash"->"Dirk Nowitzki"@0]       |
      |                                         |                     | [:like "Vince Carter"->"Tracy McGrady"@0]   | ("Lakers")       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]   | ("Nets")              | [:like "Danny Green"->"Marco Belinelli"@0]       | ("Kevin Durant")      | [:serve "Steve Nash"->"Lakers"@0]             |
      |                                         |                     | [:like "Tracy McGrady"->"Grant Hill"@0]     |                  | [:serve "Vince Carter"->"Grizzlies"@0]      | ("Grizzlies")         | [:like "Dejounte Murray"->"Marco Belinelli"@0]   | ("James Harden")      | [:serve "Steve Nash"->"Suns"@0]               |
      |                                         |                     | [:like "Tracy McGrady"->"Kobe Bryant"@0]    |                  | [:serve "Vince Carter"->"Hawks"@0]          | ("Nuggets")           | [:like "Marco Belinelli"->"Danny Green"@0]       | ("Chris Paul")        | [:serve "Steve Nash"->"Suns"@1]               |
      |                                         |                     | [:like "Tracy McGrady"->"Rudy Gay"@0]       |                  | [:serve "Vince Carter"->"Kings"@0]          | ("Warriors")          | [:like "Marco Belinelli"->"Tony Parker"@0]       | ("Bucks")             | [:like "James Harden"->"Russell Westbrook"@0] |
      |                                         |                     |                                             |                  | [:serve "Vince Carter"->"Magic"@0]          | ("Hawks")             | [:serve "Tony Parker"->"Hornets"@0]              |                       | [:serve "Kyle Anderson"->"Grizzlies"@0]       |
      |                                         |                     |                                             |                  | [:serve "Vince Carter"->"Mavericks"@0]      | ("Mavericks")         | [:serve "Tony Parker"->"Spurs"@0]                |                       | [:teammate "Tony Parker"->"Kyle Anderson"@0]  |
      |                                         |                     |                                             |                  | [:serve "Vince Carter"->"Nets"@0]           | ("Pistons")           | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |                       | [:like "Russell Westbrook"->"James Harden"@0] |
      |                                         |                     |                                             |                  | [:serve "Vince Carter"->"Raptors"@0]        | ("Clippers")          | [:teammate "Tim Duncan"->"Tony Parker"@0]        |                       | [:serve "Marc Gasol"->"Grizzlies"@0]          |
      |                                         |                     |                                             |                  | [:serve "Vince Carter"->"Suns"@0]           |                       | [:like "Boris Diaw"->"Tony Parker"@0]            |                       | [:serve "Marc Gasol"->"Raptors"@0]            |
      |                                         |                     |                                             |                  | [:like "Jason Kidd"->"Vince Carter"@0]      |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       |                       | [:serve "Chris Paul"->"Hornets"@0]            |
      |                                         |                     |                                             |                  | [:like "Vince Carter"->"Jason Kidd"@0]      |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     |                       | [:serve "Kevin Durant"->"Warriors"@0]         |
      |                                         |                     |                                             |                  | [:serve "Tim Duncan"->"Spurs"@0]            |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     |                       | [:serve "Chris Paul"->"Clippers"@0]           |
      |                                         |                     |                                             |                  | [:teammate "Manu Ginobili"->"Tim Duncan"@0] |                       | [:like "Tony Parker"->"Manu Ginobili"@0]         |                       |                                               |
      |                                         |                     |                                             |                  | [:teammate "Tony Parker"->"Tim Duncan"@0]   |                       | [:serve "Tiago Splitter"->"76ers"@0]             |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Aron Baynes"->"Tim Duncan"@0]       |                       | [:serve "Tiago Splitter"->"Hawks"@0]             |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Boris Diaw"->"Tim Duncan"@0]        |                       | [:serve "Tiago Splitter"->"Spurs"@0]             |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Danny Green"->"Tim Duncan"@0]       |                       | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Dejounte Murray"->"Tim Duncan"@0]   |                       | [:serve "Paul Gasol"->"Bucks"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  | [:like "LaMarcus Aldridge"->"Tim Duncan"@0] |                       | [:serve "Paul Gasol"->"Bulls"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Manu Ginobili"->"Tim Duncan"@0]     |                       | [:serve "Paul Gasol"->"Grizzlies"@0]             |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Marco Belinelli"->"Tim Duncan"@0]   |                       | [:serve "Paul Gasol"->"Lakers"@0]                |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tiago Splitter"->"Tim Duncan"@0]    |                       | [:serve "Paul Gasol"->"Spurs"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tony Parker"->"Tim Duncan"@0]       |                       | [:like "Marc Gasol"->"Paul Gasol"@0]             |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:like "Paul Gasol"->"Marc Gasol"@0]             |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Tim Duncan"->"Tony Parker"@0]       |                       | [:serve "Manu Ginobili"->"Spurs"@0]              |                       |                                               |
      |                                         |                     |                                             |                  | [:serve "Kobe Bryant"->"Lakers"@0]          |                       | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |                       |                                               |
      |                                         |                     |                                             |                  | [:like "Paul Gasol"->"Kobe Bryant"@0]       |                       | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Cavaliers"@0]            |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Raptors"@0]              |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Danny Green"->"Spurs"@0]                |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tim Duncan"->"Danny Green"@0]        |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Danny Green"@0]       |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Danny Green"->"LeBron James"@0]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Hawks"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Hornets"@0]               |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Jazz"@0]                  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Spurs"@0]                 |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Boris Diaw"->"Suns"@0]                  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Chris Paul"@0]        |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"James Harden"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Kevin Durant"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Jason Kidd"->"Knicks"@0]                |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Jason Kidd"->"Mavericks"@0]             |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Jason Kidd"->"Nets"@0]                  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Jason Kidd"->"Suns"@0]                  |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:serve "Jason Kidd"->"Mavericks"@1]             |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Dirk Nowitzki"->"Jason Kidd"@0]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Steve Nash"->"Jason Kidd"@0]             |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Jason Kidd"->"Dirk Nowitzki"@0]          |                       |                                               |
      |                                         |                     |                                             |                  |                                             |                       | [:like "Jason Kidd"->"Steve Nash"@0]             |                       |                                               |
    Then the result should be, in any order, with relax comparison:
      | _vertices      | _edges    |
      | [("Yao Ming")] | <[edge1]> |
      | <[vertex2]>    | <[edge2]> |
      | <[vertex3]>    | <[edge3]> |
      | <[vertex4]>    | <[edge4]> |
      | <[vertex5]>    | <[edge5]> |
    When executing query:
      """
      get subgraph 5 steps from 'Tony Parker' IN teammate OUT serve BOTH like
      """
    Then define some list variables:
      | edge1                                        | vertex2               | edge2                                            | vertex3               | edge3                                         | vertex4             | edge4                                         | vertex5                | edge5                                        | vertex6        | edge6                                |
      | [:serve "Tony Parker"->"Hornets"@0]          | ("Tim Duncan")        | [:serve "Tim Duncan"->"Spurs"@0]                 | ("Damian Lillard")    | [:serve "Damian Lillard"->"Trail Blazers"@0]  | ("Paul George")     | [:serve "Paul George"->"Pacers"@0]            | ("Dirk Nowitzki")      | [:serve "Dirk Nowitzki"->"Mavericks"@0]      | ("Nets")       | [:serve "Paul Gasol"->"Bulls"@0]     |
      | [:serve "Tony Parker"->"Spurs"@0]            | ("Boris Diaw")        | [:teammate "Manu Ginobili"->"Tim Duncan"@0]      | ("Kevin Durant")      | [:serve "Kevin Durant"->"Thunders"@0]         | ("Kyrie Irving")    | [:serve "Paul George"->"Thunders"@0]          | ("Rajon Rondo")        | [:like "Jason Kidd"->"Dirk Nowitzki"@0]      | ("Paul Gasol") | [:serve "Jason Kidd"->"Nets"@0]      |
      | [:teammate "Manu Ginobili"->"Tony Parker"@0] | ("LaMarcus Aldridge") | [:teammate "Tony Parker"->"Tim Duncan"@0]        | ("LeBron James")      | [:serve "Kevin Durant"->"Warriors"@0]         | ("Dwyane Wade")     | [:serve "Kyrie Irving"->"Cavaliers"@0]        | ("Kristaps Porzingis") | [:like "Steve Nash"->"Dirk Nowitzki"@0]      | ("Steve Nash") | [:serve "Paul Gasol"->"Bucks"@0]     |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]    | ("Manu Ginobili")     | [:like "Aron Baynes"->"Tim Duncan"@0]            | ("Danny Green")       | [:serve "LeBron James"->"Cavaliers"@0]        | ("Tracy McGrady")   | [:serve "Kyrie Irving"->"Celtics"@0]          | ("Grant Hill")         | [:like "Dirk Nowitzki"->"Jason Kidd"@0]      | ("Pelicans")   | [:serve "Paul Gasol"->"Grizzlies"@0] |
      | [:like "Boris Diaw"->"Tony Parker"@0]        | ("Marco Belinelli")   | [:like "Boris Diaw"->"Tim Duncan"@0]             | ("Kyle Anderson")     | [:serve "LeBron James"->"Heat"@0]             | ("Carmelo Anthony") | [:serve "Dwyane Wade"->"Bulls"@0]             | ("Kobe Bryant")        | [:like "Dirk Nowitzki"->"Steve Nash"@0]      | ("Jason Kidd") | [:serve "Paul Gasol"->"Lakers"@0]    |
      | [:like "Dejounte Murray"->"Tony Parker"@0]   | ("Dejounte Murray")   | [:like "Danny Green"->"Tim Duncan"@0]            | ("Tiago Splitter")    | [:serve "LeBron James"->"Lakers"@0]           | ("Yao Ming")        | [:serve "Dwyane Wade"->"Cavaliers"@0]         | ("Vince Carter")       | [:serve "Rajon Rondo"->"Bulls"@0]            |                | [:serve "Paul Gasol"->"Spurs"@0]     |
      | [:like "LaMarcus Aldridge"->"Tony Parker"@0] | ("Hornets")           | [:like "Dejounte Murray"->"Tim Duncan"@0]        | ("Russell Westbrook") | [:serve "LeBron James"->"Cavaliers"@1]        | ("JaVale McGee")    | [:serve "Dwyane Wade"->"Heat"@0]              | ("Nuggets")            | [:serve "Rajon Rondo"->"Celtics"@0]          |                | [:serve "Steve Nash"->"Lakers"@0]    |
      | [:like "Marco Belinelli"->"Tony Parker"@0]   | ("Spurs")             | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]      | ("Aron Baynes")       | [:like "Carmelo Anthony"->"LeBron James"@0]   | ("Ray Allen")       | [:serve "Dwyane Wade"->"Heat"@1]              | ("Mavericks")          | [:serve "Rajon Rondo"->"Kings"@0]            |                | [:serve "Jason Kidd"->"Suns"@0]      |
      | [:like "Tim Duncan"->"Tony Parker"@0]        |                       | [:like "Manu Ginobili"->"Tim Duncan"@0]          | ("Chris Paul")        | [:like "Chris Paul"->"LeBron James"@0]        | ("Luka Doncic")     | [:like "Carmelo Anthony"->"Dwyane Wade"@0]    | ("Pacers")             | [:serve "Rajon Rondo"->"Lakers"@0]           |                | [:serve "Steve Nash"->"Suns"@0]      |
      | [:like "Tony Parker"->"LaMarcus Aldridge"@0] |                       | [:like "Marco Belinelli"->"Tim Duncan"@0]        | ("James Harden")      | [:like "Danny Green"->"LeBron James"@0]       | ("Blake Griffin")   | [:like "Dirk Nowitzki"->"Dwyane Wade"@0]      | ("Knicks")             | [:serve "Rajon Rondo"->"Mavericks"@0]        |                | [:serve "Steve Nash"->"Suns"@1]      |
      | [:like "Tony Parker"->"Manu Ginobili"@0]     |                       | [:like "Shaquile O'Neal"->"Tim Duncan"@0]        | ("Shaquile O'Neal")   | [:like "Dwyane Wade"->"LeBron James"@0]       | ("Cavaliers")       | [:like "Dwyane Wade"->"Carmelo Anthony"@0]    | ("Bucks")              | [:serve "Rajon Rondo"->"Pelicans"@0]         |                | [:serve "Jason Kidd"->"Mavericks"@0] |
      | [:like "Tony Parker"->"Tim Duncan"@0]        |                       | [:like "Tiago Splitter"->"Tim Duncan"@0]         | ("Rudy Gay")          | [:like "Kyrie Irving"->"LeBron James"@0]      | ("Lakers")          | [:serve "Tracy McGrady"->"Magic"@0]           | ("Wizards")            | [:serve "Kristaps Porzingis"->"Knicks"@0]    |                | [:serve "Steve Nash"->"Mavericks"@0] |
      |                                              |                       | [:like "Tim Duncan"->"Manu Ginobili"@0]          | ("Suns")              | [:like "LeBron James"->"Ray Allen"@0]         | ("Heat")            | [:serve "Tracy McGrady"->"Raptors"@0]         |                        | [:serve "Kristaps Porzingis"->"Mavericks"@0] |                | [:serve "Jason Kidd"->"Mavericks"@1] |
      |                                              |                       | [:serve "Boris Diaw"->"Hawks"@0]                 | ("Jazz")              | [:serve "Danny Green"->"Cavaliers"@0]         | ("Pistons")         | [:serve "Tracy McGrady"->"Rockets"@0]         |                        | [:serve "Grant Hill"->"Clippers"@0]          |                | [:like "Steve Nash"->"Jason Kidd"@0] |
      |                                              |                       | [:serve "Boris Diaw"->"Hornets"@0]               | ("76ers")             | [:serve "Danny Green"->"Raptors"@0]           | ("Magic")           | [:serve "Tracy McGrady"->"Spurs"@0]           |                        | [:serve "Grant Hill"->"Magic"@0]             |                | [:like "Jason Kidd"->"Steve Nash"@0] |
      |                                              |                       | [:serve "Boris Diaw"->"Jazz"@0]                  | ("Hawks")             | [:serve "Danny Green"->"Spurs"@0]             | ("Celtics")         | [:like "Grant Hill"->"Tracy McGrady"@0]       |                        | [:serve "Grant Hill"->"Pistons"@0]           |                | [:serve "Jason Kidd"->"Knicks"@0]    |
      |                                              |                       | [:serve "Boris Diaw"->"Spurs"@0]                 | ("Warriors")          | [:teammate "Tim Duncan"->"Danny Green"@0]     | ("Grizzlies")       | [:like "Vince Carter"->"Tracy McGrady"@0]     |                        | [:serve "Grant Hill"->"Suns"@0]              |                |                                      |
      |                                              |                       | [:serve "Boris Diaw"->"Suns"@0]                  | ("Kings")             | [:serve "Kyle Anderson"->"Grizzlies"@0]       | ("Thunders")        | [:like "Yao Ming"->"Tracy McGrady"@0]         |                        | [:serve "Kobe Bryant"->"Lakers"@0]           |                |                                      |
      |                                              |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          | ("Trail Blazers")     | [:serve "Kyle Anderson"->"Spurs"@0]           | ("Rockets")         | [:like "Tracy McGrady"->"Grant Hill"@0]       |                        | [:like "Paul Gasol"->"Kobe Bryant"@0]        |                |                                      |
      |                                              |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0]  | ("Raptors")           | [:teammate "Tony Parker"->"Kyle Anderson"@0]  | ("Clippers")        | [:like "Tracy McGrady"->"Kobe Bryant"@0]      |                        | [:serve "Vince Carter"->"Grizzlies"@0]       |                |                                      |
      |                                              |                       | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0]  | ("Bulls")             | [:serve "Tiago Splitter"->"76ers"@0]          |                     | [:serve "Carmelo Anthony"->"Knicks"@0]        |                        | [:serve "Vince Carter"->"Hawks"@0]           |                |                                      |
      |                                              |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |                       | [:serve "Tiago Splitter"->"Hawks"@0]          |                     | [:serve "Carmelo Anthony"->"Nuggets"@0]       |                        | [:serve "Vince Carter"->"Kings"@0]           |                |                                      |
      |                                              |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  |                       | [:serve "Tiago Splitter"->"Spurs"@0]          |                     | [:serve "Carmelo Anthony"->"Rockets"@0]       |                        | [:serve "Vince Carter"->"Magic"@0]           |                |                                      |
      |                                              |                       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]        |                       | [:serve "Russell Westbrook"->"Thunders"@0]    |                     | [:serve "Carmelo Anthony"->"Thunders"@0]      |                        | [:serve "Vince Carter"->"Mavericks"@0]       |                |                                      |
      |                                              |                       | [:serve "Manu Ginobili"->"Spurs"@0]              |                       | [:like "James Harden"->"Russell Westbrook"@0] |                     | [:serve "Yao Ming"->"Rockets"@0]              |                        | [:serve "Vince Carter"->"Nets"@0]            |                |                                      |
      |                                              |                       | [:teammate "Tim Duncan"->"Manu Ginobili"@0]      |                       | [:like "Paul George"->"Russell Westbrook"@0]  |                     | [:serve "JaVale McGee"->"Lakers"@0]           |                        | [:serve "Vince Carter"->"Raptors"@0]         |                |                                      |
      |                                              |                       | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |                       | [:like "Russell Westbrook"->"James Harden"@0] |                     | [:serve "JaVale McGee"->"Mavericks"@0]        |                        | [:serve "Vince Carter"->"Suns"@0]            |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |                       | [:like "Russell Westbrook"->"Paul George"@0]  |                     | [:serve "JaVale McGee"->"Nuggets"@0]          |                        | [:like "Jason Kidd"->"Vince Carter"@0]       |                |                                      |
      |                                              |                       | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |                       | [:serve "Aron Baynes"->"Celtics"@0]           |                     | [:serve "JaVale McGee"->"Warriors"@0]         |                        | [:like "Vince Carter"->"Jason Kidd"@0]       |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"76ers"@0]            |                       | [:serve "Aron Baynes"->"Pistons"@0]           |                     | [:serve "JaVale McGee"->"Wizards"@0]          |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Bulls"@0]            |                       | [:serve "Aron Baynes"->"Spurs"@0]             |                     | [:serve "Ray Allen"->"Bucks"@0]               |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hawks"@0]            |                       | [:serve "Chris Paul"->"Clippers"@0]           |                     | [:serve "Ray Allen"->"Celtics"@0]             |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hornets"@0]          |                       | [:serve "Chris Paul"->"Hornets"@0]            |                     | [:serve "Ray Allen"->"Heat"@0]                |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Kings"@0]            |                       | [:serve "Chris Paul"->"Rockets"@0]            |                     | [:serve "Ray Allen"->"Thunders"@0]            |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Raptors"@0]          |                       | [:like "Blake Griffin"->"Chris Paul"@0]       |                     | [:like "Rajon Rondo"->"Ray Allen"@0]          |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |                       | [:like "Carmelo Anthony"->"Chris Paul"@0]     |                     | [:like "Ray Allen"->"Rajon Rondo"@0]          |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Warriors"@0]         |                       | [:like "Dwyane Wade"->"Chris Paul"@0]         |                     | [:serve "Luka Doncic"->"Mavericks"@0]         |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Hornets"@1]          |                       | [:like "Chris Paul"->"Carmelo Anthony"@0]     |                     | [:like "Kristaps Porzingis"->"Luka Doncic"@0] |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |                       | [:like "Chris Paul"->"Dwyane Wade"@0]         |                     | [:like "Luka Doncic"->"Dirk Nowitzki"@0]      |                        |                                              |                |                                      |
      |                                              |                       | [:like "Danny Green"->"Marco Belinelli"@0]       |                       | [:serve "James Harden"->"Rockets"@0]          |                     | [:like "Luka Doncic"->"Kristaps Porzingis"@0] |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |                       | [:serve "James Harden"->"Thunders"@0]         |                     | [:serve "Blake Griffin"->"Clippers"@0]        |                        |                                              |                |                                      |
      |                                              |                       | [:like "Marco Belinelli"->"Danny Green"@0]       |                       | [:like "Luka Doncic"->"James Harden"@0]       |                     | [:serve "Blake Griffin"->"Pistons"@0]         |                        |                                              |                |                                      |
      |                                              |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |                       | [:serve "Shaquile O'Neal"->"Cavaliers"@0]     |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Chris Paul"@0]        |                       | [:serve "Shaquile O'Neal"->"Celtics"@0]       |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Danny Green"@0]       |                       | [:serve "Shaquile O'Neal"->"Heat"@0]          |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"James Harden"@0]      |                       | [:serve "Shaquile O'Neal"->"Lakers"@0]        |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Kevin Durant"@0]      |                       | [:serve "Shaquile O'Neal"->"Magic"@0]         |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     |                       | [:serve "Shaquile O'Neal"->"Suns"@0]          |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |                       | [:like "Yao Ming"->"Shaquile O'Neal"@0]       |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |                       | [:like "Shaquile O'Neal"->"JaVale McGee"@0]   |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:serve "Rudy Gay"->"Grizzlies"@0]            |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:serve "Rudy Gay"->"Kings"@0]                |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:serve "Rudy Gay"->"Raptors"@0]              |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:serve "Rudy Gay"->"Spurs"@0]                |                     |                                               |                        |                                              |                |                                      |
      |                                              |                       |                                                  |                       | [:like "Tracy McGrady"->"Rudy Gay"@0]         |                     |                                               |                        |                                              |                |                                      |
    Then the result should be, in any order, with relax comparison:
      | _vertices         | _edges    |
      | [("Tony Parker")] | <[edge1]> |
      | <[vertex2]>       | <[edge2]> |
      | <[vertex3]>       | <[edge3]> |
      | <[vertex4]>       | <[edge4]> |
      | <[vertex5]>       | <[edge5]> |
      | <[vertex6]>       | <[edge6]> |

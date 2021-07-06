# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Test get subgraph before pipe

  Background:
    Given a graph with space named "nba"

  # Fix https://github.com/vesoft-inc/nebula-graph/issues/1168
  Scenario: subgraph with limit
    When executing query:
      """
      GET SUBGRAPH WITH PROP FROM 'Tim Duncan' | LIMIT 1
      """
    Then define some list variables:
      | edge1                                           |
      | [:teammate "Manu Ginobili"->"Tim Duncan"@0]     |
      | [:teammate "Tony Parker"->"Tim Duncan"@0]       |
      | [:like "Aron Baynes"->"Tim Duncan"@0]           |
      | [:like "Boris Diaw"->"Tim Duncan"@0]            |
      | [:like "Danny Green"->"Tim Duncan"@0]           |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]       |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]     |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]         |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]       |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]      |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]        |
      | [:like "Tony Parker"->"Tim Duncan"@0]           |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]         |
      | [:like "Tim Duncan"->"Tony Parker"@0]           |
      | [:serve "Tim Duncan"->"Spurs"@0]                |
      | [:teammate "Tim Duncan"->"Danny Green"@0]       |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0] |
      | [:teammate "Tim Duncan"->"Manu Ginobili"@0]     |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]       |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |

  Scenario: subgraph as variable with limit
    When executing query:
      """
      $a = GET SUBGRAPH WITH PROP FROM 'Tim Duncan' | LIMIT 1
      """
    Then define some list variables:
      | edge1                                           |
      | [:teammate "Manu Ginobili"->"Tim Duncan"@0]     |
      | [:teammate "Tony Parker"->"Tim Duncan"@0]       |
      | [:like "Aron Baynes"->"Tim Duncan"@0]           |
      | [:like "Boris Diaw"->"Tim Duncan"@0]            |
      | [:like "Danny Green"->"Tim Duncan"@0]           |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]       |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]     |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]         |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]       |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]      |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]        |
      | [:like "Tony Parker"->"Tim Duncan"@0]           |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]         |
      | [:like "Tim Duncan"->"Tony Parker"@0]           |
      | [:serve "Tim Duncan"->"Spurs"@0]                |
      | [:teammate "Tim Duncan"->"Danny Green"@0]       |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0] |
      | [:teammate "Tim Duncan"->"Manu Ginobili"@0]     |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]       |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |

  # TODO: access to the output of get subgraph.
  # Currently _vertices is a reserved keyword.
  #
  # Scenario: two steps subgraph with limit
  # When executing query:
  # """
  # (root@nebula) [nba]> $a = GET SUBGRAPH WITH PROP 3 STEPS FROM 'Paul George' OUT serve BOTH like | yield COUNT($a._vertices)
  # """
  # Then the result should be, in any order, with relax comparison:
  # | COUNT($a._vertices)    |
  # | 4                      |
  Scenario: two steps subgraph with limit
    When executing query:
      """
      GET SUBGRAPH WITH PROP 2 STEPS FROM 'Tim Duncan' | LIMIT 2
      """
    Then define some list variables:
      | edge1                                           | vertex2               | edge2                                            |
      | [:teammate "Manu Ginobili"->"Tim Duncan"@0]     | ("Danny Green")       | [:like "Dejounte Murray"->"Danny Green"@0]       |
      | [:teammate "Tony Parker"->"Tim Duncan"@0]       | ("Manu Ginobili")     | [:like "Marco Belinelli"->"Danny Green"@0]       |
      | [:like "Aron Baynes"->"Tim Duncan"@0]           | ("Aron Baynes")       | [:like "Danny Green"->"LeBron James"@0]          |
      | [:like "Boris Diaw"->"Tim Duncan"@0]            | ("Boris Diaw")        | [:like "Danny Green"->"Marco Belinelli"@0]       |
      | [:like "Danny Green"->"Tim Duncan"@0]           | ("Shaquile O\'Neal")  | [:serve "Danny Green"->"Cavaliers"@0]            |
      | [:like "Dejounte Murray"->"Tim Duncan"@0]       | ("Tony Parker")       | [:serve "Danny Green"->"Raptors"@0]              |
      | [:like "LaMarcus Aldridge"->"Tim Duncan"@0]     | ("Spurs")             | [:serve "Danny Green"->"Spurs"@0]                |
      | [:like "Manu Ginobili"->"Tim Duncan"@0]         | ("Dejounte Murray")   | [:teammate "Tony Parker"->"Manu Ginobili"@0]     |
      | [:like "Marco Belinelli"->"Tim Duncan"@0]       | ("LaMarcus Aldridge") | [:like "Dejounte Murray"->"Manu Ginobili"@0]     |
      | [:like "Shaquile O\'Neal"->"Tim Duncan"@0]      | ("Marco Belinelli")   | [:like "Tiago Splitter"->"Manu Ginobili"@0]      |
      | [:like "Tiago Splitter"->"Tim Duncan"@0]        | ("Tiago Splitter")    | [:like "Tony Parker"->"Manu Ginobili"@0]         |
      | [:like "Tony Parker"->"Tim Duncan"@0]           |                       | [:serve "Manu Ginobili"->"Spurs"@0]              |
      | [:like "Tim Duncan"->"Manu Ginobili"@0]         |                       | [:teammate "Manu Ginobili"->"Tony Parker"@0]     |
      | [:like "Tim Duncan"->"Tony Parker"@0]           |                       | [:serve "Aron Baynes"->"Celtics"@0]              |
      | [:serve "Tim Duncan"->"Spurs"@0]                |                       | [:serve "Aron Baynes"->"Pistons"@0]              |
      | [:teammate "Tim Duncan"->"Danny Green"@0]       |                       | [:serve "Aron Baynes"->"Spurs"@0]                |
      | [:teammate "Tim Duncan"->"LaMarcus Aldridge"@0] |                       | [:like "Boris Diaw"->"Tony Parker"@0]            |
      | [:teammate "Tim Duncan"->"Manu Ginobili"@0]     |                       | [:serve "Boris Diaw"->"Hawks"@0]                 |
      | [:teammate "Tim Duncan"->"Tony Parker"@0]       |                       | [:serve "Boris Diaw"->"Hornets"@0]               |
      |                                                 |                       | [:serve "Boris Diaw"->"Jazz"@0]                  |
      |                                                 |                       | [:serve "Boris Diaw"->"Spurs"@0]                 |
      |                                                 |                       | [:serve "Boris Diaw"->"Suns"@0]                  |
      |                                                 |                       | [:like "Yao Ming"->"Shaquile O\'Neal"@0]         |
      |                                                 |                       | [:like "Shaquile O\'Neal"->"JaVale McGee"@0]     |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Cavaliers"@0]       |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Celtics"@0]         |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Heat"@0]            |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Lakers"@0]          |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Magic"@0]           |
      |                                                 |                       | [:serve "Shaquile O\'Neal"->"Suns"@0]            |
      |                                                 |                       | [:like "Dejounte Murray"->"Tony Parker"@0]       |
      |                                                 |                       | [:like "LaMarcus Aldridge"->"Tony Parker"@0]     |
      |                                                 |                       | [:like "Marco Belinelli"->"Tony Parker"@0]       |
      |                                                 |                       | [:like "Tony Parker"->"LaMarcus Aldridge"@0]     |
      |                                                 |                       | [:serve "Tony Parker"->"Hornets"@0]              |
      |                                                 |                       | [:serve "Tony Parker"->"Spurs"@0]                |
      |                                                 |                       | [:teammate "Tony Parker"->"Kyle Anderson"@0]     |
      |                                                 |                       | [:teammate "Tony Parker"->"LaMarcus Aldridge"@0] |
      |                                                 |                       | [:serve "Cory Joseph"->"Spurs"@0]                |
      |                                                 |                       | [:serve "David West"->"Spurs"@0]                 |
      |                                                 |                       | [:serve "Dejounte Murray"->"Spurs"@0]            |
      |                                                 |                       | [:serve "Jonathon Simmons"->"Spurs"@0]           |
      |                                                 |                       | [:serve "Kyle Anderson"->"Spurs"@0]              |
      |                                                 |                       | [:serve "LaMarcus Aldridge"->"Spurs"@0]          |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@0]            |
      |                                                 |                       | [:serve "Paul Gasol"->"Spurs"@0]                 |
      |                                                 |                       | [:serve "Rudy Gay"->"Spurs"@0]                   |
      |                                                 |                       | [:serve "Tiago Splitter"->"Spurs"@0]             |
      |                                                 |                       | [:serve "Tracy McGrady"->"Spurs"@0]              |
      |                                                 |                       | [:serve "Marco Belinelli"->"Spurs"@1]            |
      |                                                 |                       | [:like "Dejounte Murray"->"Chris Paul"@0]        |
      |                                                 |                       | [:like "Dejounte Murray"->"James Harden"@0]      |
      |                                                 |                       | [:like "Dejounte Murray"->"Kevin Durant"@0]      |
      |                                                 |                       | [:like "Dejounte Murray"->"Kyle Anderson"@0]     |
      |                                                 |                       | [:like "Dejounte Murray"->"LeBron James"@0]      |
      |                                                 |                       | [:like "Dejounte Murray"->"Marco Belinelli"@0]   |
      |                                                 |                       | [:like "Dejounte Murray"->"Russell Westbrook"@0] |
      |                                                 |                       | [:like "Damian Lillard"->"LaMarcus Aldridge"@0]  |
      |                                                 |                       | [:like "Rudy Gay"->"LaMarcus Aldridge"@0]        |
      |                                                 |                       | [:serve "LaMarcus Aldridge"->"Trail Blazers"@0]  |
      |                                                 |                       | [:serve "Marco Belinelli"->"76ers"@0]            |
      |                                                 |                       | [:serve "Marco Belinelli"->"Bulls"@0]            |
      |                                                 |                       | [:serve "Marco Belinelli"->"Hawks"@0]            |
      |                                                 |                       | [:serve "Marco Belinelli"->"Hornets"@0]          |
      |                                                 |                       | [:serve "Marco Belinelli"->"Kings"@0]            |
      |                                                 |                       | [:serve "Marco Belinelli"->"Raptors"@0]          |
      |                                                 |                       | [:serve "Marco Belinelli"->"Warriors"@0]         |
      |                                                 |                       | [:serve "Marco Belinelli"->"Hornets"@1]          |
      |                                                 |                       | [:serve "Tiago Splitter"->"76ers"@0]             |
      |                                                 |                       | [:serve "Tiago Splitter"->"Hawks"@0]             |
    Then the result should be, in any order, with relax comparison:
      | _vertices        | _edges    |
      | [("Tim Duncan")] | <[edge1]> |
      | <[vertex2]>      | <[edge2]> |

  Scenario: three steps subgraph with property + direction + limit
    When executing query:
      """
      GET SUBGRAPH WITH PROP 3 STEPS FROM 'Paul George' OUT serve BOTH like | LIMIT 2
      """
    Then define some list variables:
      | edge1                                        | edge2                                            |
      | [:like "Russell Westbrook"->"Paul George"@0] | [:like "Dejounte Murray"->"Russell Westbrook"@0] |
      | [:serve "Paul George"->"Pacers"@0]           | [:like "James Harden"->"Russell Westbrook"@0]    |
      | [:serve "Paul George"->"Thunders"@0]         | [:serve "Russell Westbrook"->"Thunders"@0]       |
      | [:like "Paul George"->"Russell Westbrook"@0] | [:like "Russell Westbrook"->"James Harden"@0]    |
    Then the result should be, in any order, with relax comparison:
      | _vertices                                         | _edges    |
      | [("Paul George")]                                 | <[edge1]> |
      | [("Russell Westbrook"), ("Pacers"), ("Thunders")] | <[edge2]> |
    When executing query:
      """
      GET SUBGRAPH WITH PROP 3 STEPS FROM 'Paul George' OUT serve BOTH like | YIELD COUNT(*)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(*) |
      | 4        |

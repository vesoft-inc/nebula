# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Test match used in pipe

  Background:
    Given a graph with space named "nba"

  Scenario: Order by after match
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[]-(m) RETURN n,m | ORDER BY $-.m;
      """
    Then the result should be, in any order, with relax comparison:
      | n              | m                     |
      | ("Tim Duncan") | ("Aron Baynes")       |
      | ("Tim Duncan") | ("Boris Diaw")        |
      | ("Tim Duncan") | ("Danny Green")       |
      | ("Tim Duncan") | ("Danny Green")       |
      | ("Tim Duncan") | ("Dejounte Murray")   |
      | ("Tim Duncan") | ("LaMarcus Aldridge") |
      | ("Tim Duncan") | ("LaMarcus Aldridge") |
      | ("Tim Duncan") | ("Manu Ginobili")     |
      | ("Tim Duncan") | ("Manu Ginobili")     |
      | ("Tim Duncan") | ("Manu Ginobili")     |
      | ("Tim Duncan") | ("Manu Ginobili")     |
      | ("Tim Duncan") | ("Marco Belinelli")   |
      | ("Tim Duncan") | ("Shaquile O'Neal")   |
      | ("Tim Duncan") | ("Spurs")             |
      | ("Tim Duncan") | ("Tiago Splitter")    |
      | ("Tim Duncan") | ("Tony Parker")       |
      | ("Tim Duncan") | ("Tony Parker")       |
      | ("Tim Duncan") | ("Tony Parker")       |
      | ("Tim Duncan") | ("Tony Parker")       |

  Scenario: Group after match
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[]-(m) RETURN n,m | GROUP BY $-.n, $-.m YIELD $-.n, $-.m, count(*);
      """
    Then the result should be, in any order, with relax comparison:
      | $-.n           | $-.m                  | count(*) |
      | ("Tim Duncan") | ("Spurs")             | 1        |
      | ("Tim Duncan") | ("Shaquile O'Neal")   | 1        |
      | ("Tim Duncan") | ("Tiago Splitter")    | 1        |
      | ("Tim Duncan") | ("Marco Belinelli")   | 1        |
      | ("Tim Duncan") | ("Dejounte Murray")   | 1        |
      | ("Tim Duncan") | ("Tony Parker")       | 4        |
      | ("Tim Duncan") | ("Danny Green")       | 2        |
      | ("Tim Duncan") | ("Manu Ginobili")     | 4        |
      | ("Tim Duncan") | ("Aron Baynes")       | 1        |
      | ("Tim Duncan") | ("LaMarcus Aldridge") | 2        |
      | ("Tim Duncan") | ("Boris Diaw")        | 1        |

  Scenario: Top n after match
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[]-(m) RETURN n,m | ORDER BY $-.m | LIMIT 10;
      """
    Then the result should be, in any order, with relax comparison:
      | n              | m                     |
      | ("Tim Duncan") | ("Aron Baynes")       |
      | ("Tim Duncan") | ("Boris Diaw")        |
      | ("Tim Duncan") | ("Danny Green")       |
      | ("Tim Duncan") | ("Danny Green")       |
      | ("Tim Duncan") | ("Dejounte Murray")   |
      | ("Tim Duncan") | ("LaMarcus Aldridge") |
      | ("Tim Duncan") | ("LaMarcus Aldridge") |
      | ("Tim Duncan") | ("Manu Ginobili")     |
      | ("Tim Duncan") | ("Manu Ginobili")     |
      | ("Tim Duncan") | ("Manu Ginobili")     |

  Scenario: Go after match
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[]-(m) RETURN n,m | GO FROM $-.n OVER *;
      """
    Then a SemanticError should be raised at runtime: `$-.n', the srcs should be type of FIXED_STRING, but was`__EMPTY__'

  Scenario: Set op after match
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"}) RETURN n UNION MATCH (n:player{name:"Tony Parker"}) RETURN n
      """
    Then the result should be, in any order, with relax comparison:
      | n               |
      | ("Tim Duncan")  |
      | ("Tony Parker") |

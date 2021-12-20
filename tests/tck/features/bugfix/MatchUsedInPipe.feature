# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test match used in pipe

  Background:
    Given a graph with space named "nba"

  Scenario: Order by after match
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[]-(m) RETURN n,m  ORDER BY m;
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
      | ("Tim Duncan") | ("Shaquille O'Neal")  |
      | ("Tim Duncan") | ("Spurs")             |
      | ("Tim Duncan") | ("Tiago Splitter")    |
      | ("Tim Duncan") | ("Tony Parker")       |
      | ("Tim Duncan") | ("Tony Parker")       |
      | ("Tim Duncan") | ("Tony Parker")       |
      | ("Tim Duncan") | ("Tony Parker")       |

  Scenario: Group after match
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"})-[]-(m)
      WITH n as a, m as b
      RETURN a, b, count(*)
      """
    Then the result should be, in any order, with relax comparison:
      | a              | b                     | count(*) |
      | ("Tim Duncan") | ("Spurs")             | 1        |
      | ("Tim Duncan") | ("Shaquille O'Neal")  | 1        |
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
      MATCH (n:player{name:"Tim Duncan"})-[]-(m) RETURN n,m ORDER BY m LIMIT 10;
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
    Then a SyntaxError should be raised at runtime: syntax error near `| GO FRO'

  Scenario: Set op after match
    When executing query:
      """
      MATCH (n:player{name:"Tim Duncan"}) RETURN n UNION MATCH (n:player{name:"Tony Parker"}) RETURN n
      """
    Then a SyntaxError should be raised at runtime: syntax error near `UNION'

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Return. A standalone return sentence is actually a yield sentence

  Background:
    Given a graph with space named "nba"

  Scenario: base
    When executing query:
      """
      RETURN 1+1 AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 2   |
    When executing query:
      """
      RETURN 1- -1 AS sub
      """
    Then the result should be, in any order:
      | sub |
      | 2   |
    When executing query:
      """
      RETURN 1--1 AS sub
      """
    Then a SyntaxError should be raised at runtime: syntax error near `--'
    When executing query:
      """
      RETURN  [2,3 ] - [3] AS sub
      """
    Then a SemanticError should be raised at runtime: `([2,3]-[3])' is not a valid expression, can not apply `-' to `LIST' and `LIST'.
    When executing query:
      """
      RETURN  [2,3 ]-[3] AS sub
      """
    Then a SyntaxError should be raised at runtime: syntax error near `]-'
    # When executing query:
    # """
    # WITH [2,3] AS a, 3 AS b, [2] AS c return (a)-[b]-(c)
    # """
    # FIXME(czp): Fix this after https://github.com/vesoft-inc/nebula/issues/5288 closed
    # Then a semanticError should be raised at runtime: `a' is defined with type Runtime, but referenced with type Node
    # When executing query:
    # """
    # WITH [2,3] AS a, 3 AS b, [2] AS c return (a)- [b] -(c)
    # """
    # FIXME(czp): Fix this after https://github.com/vesoft-inc/nebula/issues/5288 closed
    # Then a semanticError should be raised at runtime: `(a)- [b]' is not a valid expression, can not apply `-' to `LIST' and `LIST'.
    When executing query:
      """
      RETURN DISTINCT 1+1, '1+1', (int)3.14, (string)(1+1), (string)true
      """
    Then the result should be, in any order:
      | (1+1) | "1+1" | (INT)3.14 | (STRING)(1+1) | (STRING)true |
      | 2     | "1+1" | 3         | "2"           | "true"       |
    When executing query:
      """
      GO FROM "Tony Parker" OVER like YIELD id($$) AS vid | RETURN $-.vid AS dst
      """
    Then the result should be, in any order, with relax comparison:
      | dst                 |
      | "LaMarcus Aldridge" |
      | "Manu Ginobili"     |
      | "Tim Duncan"        |
    When executing query:
      """
      FETCH PROP ON player "Tony Parker" YIELD player.age as age | RETURN $-.age + 100 AS age
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 136 |

  Scenario: hash call
    When executing query:
      """
      RETURN hash("Boris")
      """
    Then the result should be, in any order:
      | hash("Boris")       |
      | 9126854228122744212 |
    When executing query:
      """
      RETURN hash(123)
      """
    Then the result should be, in any order:
      | hash(123) |
      | 123       |

  Scenario: logical
    When executing query:
      """
      RETURN NOT FALSE OR FALSE AND FALSE XOR FALSE
      """
    Then the result should be, in any order:
      | ((!(false) OR (false AND false)) XOR false) |
      | true                                        |

  Scenario: Error check
    When executing query:
      """
      RETURN count(rand32());
      """
    Then a SyntaxError should be raised at runtime: Can't use non-deterministic (random) functions inside of aggregate functions near `rand32()'
    When executing query:
      """
      RETURN avg(ranD()+1);
      """
    Then a SyntaxError should be raised at runtime: Can't use non-deterministic (random) functions inside of aggregate functions near `ranD()+1'
    When executing query:
      """
      RETURN $$.dummyTag.p
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `dummyTag`
    When executing query:
      """
      RETURN $^.dummyTag.p
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: TagName `dummyTag`
    When executing query:
      """
      RETURN $-.dummyTag.p
      """
    Then a SemanticError should be raised at runtime: `$-.dummyTag', not exist prop `dummyTag'

  Scenario: label expr
    When executing query:
      """
      RETURN name
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: name

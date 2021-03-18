Feature: Yield

  Background:
    Given an empty graph

  Scenario: base
    When executing query:
      """
      YIELD 1+1 AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 2   |
    When executing query:
      """
      YIELD 1+1, '1+1', (int)3.14, (string)(1+1), (string)true
      """
    Then the result should be, in any order:
      | (1+1) | "1+1" | (INT)3.14 | (STRING)(1+1) | (STRING)true |
      | 2     | "1+1" | 3         | "2"           | "true"       |

  Scenario: hash call
    When executing query:
      """
      YIELD hash("Boris")
      """
    Then the result should be, in any order:
      | hash("Boris")       |
      | 9126854228122744212 |
    When executing query:
      """
      YIELD hash(123)
      """
    Then the result should be, in any order:
      | hash(123) |
      | 123       |

  Scenario: logical
    When executing query:
      """
      YIELD NOT FALSE OR FALSE AND FALSE XOR FALSE
      """
    Then the result should be, in any order:
      | ((!(false) OR (false AND false)) XOR false) |
      | true                                        |

  Scenario: nested
    When executing query:
      """
      YIELD 1 AS number | YIELD $-.number AS number
      """
    Then the result should be, in any order:
      | number |
      | 1      |
    When executing query:
      """
      $a = YIELD 1 AS number;
      YIELD $a.number AS number;
      """
    Then the result should be, in any order:
      | number |
      | 1      |
    When executing query:
      """
      YIELD 1 AS number UNION YIELD 2 AS number
      """
    Then the result should be, in any order:
      | number |
      | 1      |
      | 2      |
    When executing query:
      """
      YIELD 1 AS number INTERSECT YIELD 2 AS number
      """
    Then the result should be, in any order:
      | number |
    When executing query:
      """
      YIELD 1 AS number MINUS YIELD 2 AS number
      """
    Then the result should be, in any order:
      | number |
      | 1      |

  Scenario: tagProp
    When executing query:
      """
      YIELD $$.dummyTag.p
      """
    Then a ExecutionError should be raised at runtime: TagName `dummyTag'  is nonexistent
    When executing query:
      """
      YIELD $^.dummyTag.p
      """
    Then a ExecutionError should be raised at runtime: TagName `dummyTag'  is nonexistent
    When executing query:
      """
      YIELD $-.dummyTag.p
      """
    Then a SemanticError should be raised at runtime: `$-.dummyTag', not exist prop `dummyTag'

  Scenario: label expr
    When executing query:
      """
      YIELD name
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: name

  Scenario: with go from
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve
      YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team;
      """
    Then a SemanticError should be raised at runtime: Space was not chosen.

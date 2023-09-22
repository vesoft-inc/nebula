Feature: is_inversed Function

  Background:
    Given a graph with space named "nba"

  Scenario: Test this is equivalent to typeid(e) < 0
    When executing query:
      """
      MATCH (v1:player{name:"Tim Duncan"})-[e]->(v2:player{name:"Tony Parker"})
      RETURN IS_INVERSED(e) == (TYPEID(e) < 0) AS result;
      """
    Then the result should be, in any order:
      | result |
      | true   |

  Scenario: Test Positive Cases
    When executing query:
      """
      MATCH (v1:player{name:"Tim Duncan"})-[e:serve]->(v2:team)
      RETURN IS_INVERSED(e) AS result;
      """
    Then the result should be, in any order:
      | result |
      | false  |
    When executing query:
      """
      MATCH (v1:player{name:"Tim Duncan"})-[e]-(v2:player{name:"Tony Parker"})
      RETURN IS_INVERSED(e) AS result;
      """
    Then the result should be, in any order:
      | result |
      | true   |
      | false  |
      | true   |
      | false  |
    When executing query:
      """
      GO FROM "player100" OVER follow REVERSELY YIELD is_inversed(edge) AS result
      | GROUP BY $-.result YIELD $-.result AS result, count(*) AS count
      """
    Then the result should be, in any order:
      | result | count |
      | true   | 10    |

  Scenario: Test Cases With Invalid Input
    When executing query:
      """
      YIELD IS_INVERSED("fuzz") AS result;
      """
    Then a SemanticError should be raised at runtime: `IS_INVERSED("fuzz")' is not a valid expression : Parameter's type error
    When executing query:
      """
      YIELD IS_INVERSED(3.1415926) AS result;
      """
    Then a SemanticError should be raised at runtime: `IS_INVERSED(3.1415926)' is not a valid expression : Parameter's type error

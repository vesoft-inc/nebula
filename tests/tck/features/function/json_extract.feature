Feature: json_extract Function

  Background:
    Test json_extract function

  Scenario: Test Positive Cases
    When executing query:
      """
      YIELD JSON_EXTRACT('{"a": "foo", "b": 0.2, "c": true}') AS result;
      """
    Then the result should be, in any order:
      | result                      |
      | {a: "foo", b: 0.2, c: true} |
    When executing query:
      """
      YIELD JSON_EXTRACT('{"a": 1, "b": {}, "c": {"d": true}}') AS result;
      """
    Then the result should be, in any order:
      | result                      |
      | {a: 1, b: {}, c: {d: true}} |
    When executing query:
      """
      YIELD JSON_EXTRACT('{}') AS result;
      """
    Then the result should be, in any order:
      | result |
      | {}     |

  Scenario: Test Cases With Invalid JSON String
    When executing query:
      """
      YIELD JSON_EXTRACT('fuzz') AS result;
      """
    Then the result should be, in any order:
      | result   |
      | BAD_DATA |
    When executing query:
      """
      YIELD JSON_EXTRACT(3.1415926) AS result;
      """
    Then a SemanticError should be raised at runtime: `JSON_EXTRACT(3.1415926)' is not a valid expression : Parameter's type error

  Scenario: Test Cases Hitting Limitations
    # Here nested Map depth is 2, the nested item is omitted:
    When executing query:
      """
      YIELD JSON_EXTRACT('{"a": "foo", "b": false, "c": {"d": {"e": 0.1}}}') AS result;
      """
    Then the result should be, in any order:
      | result                      |
      | {a: "foo", b: false, c: {}} |
    # Here List is not yet supported, the encounted value is omitted:
    When executing query:
      """
      YIELD JSON_EXTRACT('{"a": "foo", "b": false, "c": [1, 2, 3]}') AS result;
      """
    Then the result should be, in any order:
      | result               |
      | {a: "foo", b: false} |

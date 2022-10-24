Feature: Coalesce Function

  Background:
    Test coalesce function

  Scenario: test normal case
    When executing query:
      """
      RETURN coalesce(null,1) as result;
      """
    Then the result should be, in any order:
      | result |
      | 1      |
    When executing query:
      """
      RETURN coalesce(1,2,3) as result;
      """
    Then the result should be, in any order:
      | result |
      | 1      |
    When executing query:
      """
      RETURN coalesce(null) as result;
      """
    Then the result should be, in any order:
      | result |
      | NULL   |
    When executing query:
      """
      RETURN coalesce(null,[1,2,3]) as result;
      """
    Then the result should be, in any order:
      | result    |
      | [1, 2, 3] |
    When executing query:
      """
      RETURN coalesce(null,1.234) as result;
      """
    Then the result should be, in any order:
      | result |
      | 1.234  |

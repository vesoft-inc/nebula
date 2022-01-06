Feature: Fetch Int Vid Edges

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
      | result   |
      | __NULL__ |

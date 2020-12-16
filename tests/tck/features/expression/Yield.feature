Feature: Yield

  @skip
  Scenario: yield without chosen space
    Given an empty graph
    When executing query:
      """
      YIELD 1+1 AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 2   |

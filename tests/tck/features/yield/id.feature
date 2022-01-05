Feature: unique id

  Background:
    Yield uuid() to get id
  
  Scenario: segment id
    When executing query:
      """
      YIELD uuid()
      """
    Then the result should be, in any order:
      | uuid() |
      | 0      |

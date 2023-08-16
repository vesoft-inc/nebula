Feature: Round

    Background:
        Test round function

    Scenario: test `up` mode
        When executing query:
          """
          RETURN round(1.249, 1, "up") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.3       |
        When executing query:
          """
          RETURN round(-1.251, 1, "up") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.3      |
        When executing query:
          """
          RETURN round(1.25, 1, "up") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.3       |
        When executing query:
          """
          RETURN round(-1.35, 1, "up") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.4      |
    Scenario: test `down` mode
        When executing query:
          """
          RETURN round(1.249, 1, "down") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.251, 1, "down") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.2      |
        When executing query:
          """
          RETURN round(1.25, 1, "down") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.35, 1, "down") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.3      |
    Scenario: test `ceiling` mode
        When executing query:
          """
          RETURN round(1.249, 1, "ceiling") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.3       |
        When executing query:
          """
          RETURN round(-1.251, 1, "ceiling") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.2      |
        When executing query:
          """
          RETURN round(1.25, 1, "ceiling") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.3       |
        When executing query:
          """
          RETURN round(-1.35, 1, "ceiling") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.3      |
    Scenario: test `floor` mode
        When executing query:
          """
          RETURN round(1.249, 1, "floor") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.251, 1, "floor") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.3      |
        When executing query:
          """
          RETURN round(1.25, 1, "floor") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.35, 1, "floor") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.4      |
    Scenario: test `half_up` mode
        When executing query:
          """
          RETURN round(1.249, 1, "half_up") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.251, 1, "half_up") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.3      |
        When executing query:
          """
          RETURN round(1.25, 1, "half_up") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.3       |
        When executing query:
          """
          RETURN round(-1.35, 1, "half_up") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.4      |
    Scenario: test `half_down` mode
        When executing query:
          """
          RETURN round(1.249, 1, "half_down") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.251, 1, "half_down") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.3      |
        When executing query:
          """
          RETURN round(1.25, 1, "half_down") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.35, 1, "half_down") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.3      |
    Scenario: test `half_even` mode
        When executing query:
          """
          RETURN round(1.249, 1, "half_even") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.251, 1, "half_even") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.3      |
        When executing query:
          """
          RETURN round(1.25, 1, "half_even") as result
          """
        Then the result should be, in any order:
          | result    |
          | 1.2       |
        When executing query:
          """
          RETURN round(-1.35, 1, "half_even") as result
          """
        Then the result should be, in any order:
          | result    |
          | -1.4      |

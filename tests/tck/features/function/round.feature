Feature: Round

  Background:
    Test round function

  Scenario: test `up` mode
    When executing query:
      """
      RETURN round(1.249, 1, "up") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.3    |
    When executing query:
      """
      RETURN round(-1.251, 1, "up") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.3   |
    When executing query:
      """
      RETURN round(1.25, 1, "up") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.3    |
    When executing query:
      """
      RETURN round(-1.35, 1, "up") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.4   |

  Scenario: test `down` mode
    When executing query:
      """
      RETURN round(1.249, 1, "down") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.251, 1, "down") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.2   |
    When executing query:
      """
      RETURN round(1.25, 1, "down") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.35, 1, "down") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.3   |

  Scenario: test `ceiling` mode
    When executing query:
      """
      RETURN round(1.249, 1, "ceiling") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.3    |
    When executing query:
      """
      RETURN round(-1.251, 1, "ceiling") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.2   |
    When executing query:
      """
      RETURN round(1.25, 1, "ceiling") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.3    |
    When executing query:
      """
      RETURN round(-1.35, 1, "ceiling") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.3   |

  Scenario: test `floor` mode
    When executing query:
      """
      RETURN round(1.249, 1, "floor") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.251, 1, "floor") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.3   |
    When executing query:
      """
      RETURN round(1.25, 1, "floor") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.35, 1, "floor") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.4   |

  Scenario: test `half_up` mode
    When executing query:
      """
      RETURN round(1.249, 1, "half_up") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.251, 1, "half_up") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.3   |
    When executing query:
      """
      RETURN round(1.25, 1, "half_up") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.3    |
    When executing query:
      """
      RETURN round(-1.35, 1, "half_up") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.4   |

  Scenario: test `half_down` mode
    When executing query:
      """
      RETURN round(1.249, 1, "half_down") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.251, 1, "half_down") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.3   |
    When executing query:
      """
      RETURN round(1.25, 1, "half_down") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.35, 1, "half_down") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.3   |

  Scenario: test `half_even` mode
    When executing query:
      """
      RETURN round(1.249, 1, "half_even") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.251, 1, "half_even") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.3   |
    When executing query:
      """
      RETURN round(1.25, 1, "half_even") as result
      """
    Then the result should be, in any order:
      | result |
      | 1.2    |
    When executing query:
      """
      RETURN round(-1.35, 1, "half_even") as result
      """
    Then the result should be, in any order:
      | result |
      | -1.4   |

  Scenario: test bad_type
    When executing query:
      """
      RETURN round(3.125, 3.2) as result
      """
    Then a SemanticError should be raised at runtime: `round(3.125,3.2)' is not a valid expression : Parameter's type error
    When executing query:
      """
      RETURN round(3.125, 3.2, 42) as result
      """
    Then a SemanticError should be raised at runtime: `round(3.125,3.2,42)' is not a valid expression : Parameter's type error
    When executing query:
      """
      RETURN round("3.124", 3) as result
      """
    Then a SemanticError should be raised at runtime: `round("3.125",3)' is not a valid expression : Parameter's type error
    When executing query:
      """
      RETURN round(1.4, "fs", "half_up") as result
      """
    Then a SemanticError should be raised at runtime: `round(1.4,"fs","half_up")' is not a valid expression : Parameter's type error

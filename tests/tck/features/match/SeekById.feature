Feature: Match seek by id

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: basic
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == 'Paul Gasol'
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: basic logical not
    When executing query:
      """
      MATCH (v)
      WHERE NOT NOT id(v) == 'Paul Gasol'
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | 'Paul Gasol' | 38  |
    When executing query:
      """
      MATCH (v)
      WHERE NOT NOT id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: basic logical and
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | 'Paul Gasol' |

  Scenario: basic logical or
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') OR id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'Paul Gasol'       |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') OR id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'Paul Gasol'       |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: basic logical with noise
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) == 'Paul Gasol'
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | 'Paul Gasol' |
    When executing query:
      """
      MATCH (v)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) != 'Paul Gasol'
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
            OR false
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'Paul Gasol'       |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray', 'Paul Gasol']
            AND (id(v) == 'James Harden' OR v.age == 23)
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name           |
      | 'James Harden' |

  Scenario: complicate logical
    When executing query:
      """
      MATCH (v)
      WHERE ((NOT NOT id(v) == 'Paul Gasol')
            OR id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray'])
            AND id(v) != 'Paul Gasol'
            AND v.name != 'Jonathon Simmons'
            AND v.age == 29
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name              |
      | 'James Harden'    |
      | 'Klay Thompson'   |
      | 'Dejounte Murray' |
    When executing query:
      """
      MATCH (v)
      WHERE (id(v) == "Tim Duncan" AND v.age>10) OR (id(v) == "Tony Parker" AND v.age>10)
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name          |
      | "Tim Duncan"  |
      | "Tony Parker" |

  Scenario: with extend
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) == 'Paul Gasol'
      RETURN v.name AS Name, t.name AS Team
      """
    Then the result should be, in any order:
      | Name         | Team        |
      | 'Paul Gasol' | 'Grizzlies' |
      | 'Paul Gasol' | 'Lakers'    |
      | 'Paul Gasol' | 'Bulls'     |
      | 'Paul Gasol' | 'Spurs'     |
      | 'Paul Gasol' | 'Bucks'     |

  Scenario: multiple nodes
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE (NOT NOT id(v) == 'Paul Gasol') AND id(v) == 'Paul Gasol' AND id(t) IN ['Grizzlies', 'Lakers']
      RETURN v.name AS Name, t.name AS Team
      """
    Then the result should be, in any order:
      | Name         | Team        |
      | 'Paul Gasol' | 'Grizzlies' |
      | 'Paul Gasol' | 'Lakers'    |
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE ((NOT NOT id(v) == 'Paul Gasol') AND id(v) == 'Paul Gasol') OR id(t) IN ['Grizzlies', 'Lakers']
      RETURN v.name AS Name, t.name AS Team
      """
    Then the result should be, in any order:
      | Name         | Team        |
      | 'Paul Gasol' | 'Grizzlies' |
      | 'Paul Gasol' | 'Lakers'    |
      | 'Paul Gasol' | 'Bulls'     |
      | 'Paul Gasol' | 'Spurs'     |
      | 'Paul Gasol' | 'Bucks'     |

  Scenario: can't refer
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) == 'Paul Gasol'
      RETURN v.name AS Name, v.age AS Age
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE NOT id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE id(v) IN ['James Harden', 'Jonathon Simmons', 'Klay Thompson', 'Dejounte Murray']
            OR v.age == 23
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE id(v) == 'James Harden'
            OR v.age == 23
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE id(x) == 'James Harden'
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      MATCH (v)
      WHERE (id(v) + '') == 'James Harden'
      RETURN v.name AS Name
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Start from end
    When executing query:
      """
      MATCH (v)-[:serve]->(t)
      WHERE id(t) == 'Pistons'
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name            |
      | 'Aron Baynes'   |
      | 'Blake Griffin' |
      | 'Grant Hill'    |

Feature: Variable length relationship match

  Scenario: m to n
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (:player{name:'Tim Duncan'})-[e:serve*2..3{start_year: 2000}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e | v |
    And no side effects
    When executing query:
      """
      MATCH (:player{name:'Tim Duncan'})<-[e:like*2..3{likeness: 90}]-(v)
      RETURN e, v
      """
    Then the result should be, in any order, with relax comparision:
      | e                                                                                      | v                  |
      | [[:like "Tim Duncan"<-"Manu Ginobili"@0], [:like "Manu Ginobili"<-"Tiago Splitter"@0]] | ("Tiago Splitter") |
    And no side effects

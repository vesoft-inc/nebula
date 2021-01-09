Feature: Match seek by tag

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: seek by empty tag index
    When executing query:
      """
      MATCH (v:bachelor)
      RETURN id(v) AS vid
      """
    Then the result should be, in any order:
      | vid                |
      | hash('Tim Duncan') |
    And no side effects
    When executing query:
      """
      MATCH (v:bachelor)
      RETURN id(v) AS vid, v.age AS age
      """
    Then the result should be, in any order:
      | vid                | age |
      | hash('Tim Duncan') | 42  |
    And no side effects

  Scenario: seek by tag index
    When executing query:
      """
      MATCH (v:team)
      RETURN id(v)
      """
    Then the result should be, in any order:
      | id(v)                 |
      | hash('Nets')          |
      | hash('Pistons')       |
      | hash('Bucks')         |
      | hash('Mavericks')     |
      | hash('Clippers')      |
      | hash('Thunders')      |
      | hash('Lakers')        |
      | hash('Jazz')          |
      | hash('Nuggets')       |
      | hash('Wizards')       |
      | hash('Pacers')        |
      | hash('Timberwolves')  |
      | hash('Hawks')         |
      | hash('Warriors')      |
      | hash('Magic')         |
      | hash('Rockets')       |
      | hash('Pelicans')      |
      | hash('Raptors')       |
      | hash('Spurs')         |
      | hash('Heat')          |
      | hash('Grizzlies')     |
      | hash('Knicks')        |
      | hash('Suns')          |
      | hash('Hornets')       |
      | hash('Cavaliers')     |
      | hash('Kings')         |
      | hash('Celtics')       |
      | hash('76ers')         |
      | hash('Trail Blazers') |
      | hash('Bulls')         |
    And no side effects
    When executing query:
      """
      MATCH (v:team)
      RETURN id(v) AS vid, v.name AS name
      """
    Then the result should be, in any order:
      | vid                   | name            |
      | hash('Nets')          | 'Nets'          |
      | hash('Pistons')       | 'Pistons'       |
      | hash('Bucks')         | 'Bucks'         |
      | hash('Mavericks')     | 'Mavericks'     |
      | hash('Clippers')      | 'Clippers'      |
      | hash('Thunders')      | 'Thunders'      |
      | hash('Lakers')        | 'Lakers'        |
      | hash('Jazz')          | 'Jazz'          |
      | hash('Nuggets')       | 'Nuggets'       |
      | hash('Wizards')       | 'Wizards'       |
      | hash('Pacers')        | 'Pacers'        |
      | hash('Timberwolves')  | 'Timberwolves'  |
      | hash('Hawks')         | 'Hawks'         |
      | hash('Warriors')      | 'Warriors'      |
      | hash('Magic')         | 'Magic'         |
      | hash('Rockets')       | 'Rockets'       |
      | hash('Pelicans')      | 'Pelicans'      |
      | hash('Raptors')       | 'Raptors'       |
      | hash('Spurs')         | 'Spurs'         |
      | hash('Heat')          | 'Heat'          |
      | hash('Grizzlies')     | 'Grizzlies'     |
      | hash('Knicks')        | 'Knicks'        |
      | hash('Suns')          | 'Suns'          |
      | hash('Hornets')       | 'Hornets'       |
      | hash('Cavaliers')     | 'Cavaliers'     |
      | hash('Kings')         | 'Kings'         |
      | hash('Celtics')       | 'Celtics'       |
      | hash('76ers')         | '76ers'         |
      | hash('Trail Blazers') | 'Trail Blazers' |
      | hash('Bulls')         | 'Bulls'         |
    And no side effects

  Scenario: seek by tag index with extend
    When executing query:
      """
      MATCH (p:bachelor)-[:serve]->(t)
      RETURN t.name AS team
      """
    Then the result should be, in any order:
      | team    |
      | 'Spurs' |
    And no side effects

Feature: Match seek by tag

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: seek by empty tag index
    When executing query:
      """
      MATCH (v:bachelor)
      RETURN id(v) AS vid
      """
    Then the result should be, in any order:
      | vid          |
      | 'Tim Duncan' |
    And no side effects
    When executing query:
      """
      MATCH (v:bachelor)
      RETURN id(v) AS vid, v.age AS age
      """
    Then the result should be, in any order:
      | vid          | age |
      | 'Tim Duncan' | 42  |
    And no side effects

  Scenario: seek by tag index
    When executing query:
      """
      MATCH (v:team)
      RETURN id(v)
      """
    Then the result should be, in any order:
      | id(v)           |
      | 'Nets'          |
      | 'Pistons'       |
      | 'Bucks'         |
      | 'Mavericks'     |
      | 'Clippers'      |
      | 'Thunders'      |
      | 'Lakers'        |
      | 'Jazz'          |
      | 'Nuggets'       |
      | 'Wizards'       |
      | 'Pacers'        |
      | 'Timberwolves'  |
      | 'Hawks'         |
      | 'Warriors'      |
      | 'Magic'         |
      | 'Rockets'       |
      | 'Pelicans'      |
      | 'Raptors'       |
      | 'Spurs'         |
      | 'Heat'          |
      | 'Grizzlies'     |
      | 'Knicks'        |
      | 'Suns'          |
      | 'Hornets'       |
      | 'Cavaliers'     |
      | 'Kings'         |
      | 'Celtics'       |
      | '76ers'         |
      | 'Trail Blazers' |
      | 'Bulls'         |
    And no side effects
    When executing query:
      """
      MATCH (v:team)
      RETURN id(v) AS vid, v.name AS name
      """
    Then the result should be, in any order:
      | vid             | name            |
      | 'Nets'          | 'Nets'          |
      | 'Pistons'       | 'Pistons'       |
      | 'Bucks'         | 'Bucks'         |
      | 'Mavericks'     | 'Mavericks'     |
      | 'Clippers'      | 'Clippers'      |
      | 'Thunders'      | 'Thunders'      |
      | 'Lakers'        | 'Lakers'        |
      | 'Jazz'          | 'Jazz'          |
      | 'Nuggets'       | 'Nuggets'       |
      | 'Wizards'       | 'Wizards'       |
      | 'Pacers'        | 'Pacers'        |
      | 'Timberwolves'  | 'Timberwolves'  |
      | 'Hawks'         | 'Hawks'         |
      | 'Warriors'      | 'Warriors'      |
      | 'Magic'         | 'Magic'         |
      | 'Rockets'       | 'Rockets'       |
      | 'Pelicans'      | 'Pelicans'      |
      | 'Raptors'       | 'Raptors'       |
      | 'Spurs'         | 'Spurs'         |
      | 'Heat'          | 'Heat'          |
      | 'Grizzlies'     | 'Grizzlies'     |
      | 'Knicks'        | 'Knicks'        |
      | 'Suns'          | 'Suns'          |
      | 'Hornets'       | 'Hornets'       |
      | 'Cavaliers'     | 'Cavaliers'     |
      | 'Kings'         | 'Kings'         |
      | 'Celtics'       | 'Celtics'       |
      | '76ers'         | '76ers'         |
      | 'Trail Blazers' | 'Trail Blazers' |
      | 'Bulls'         | 'Bulls'         |
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

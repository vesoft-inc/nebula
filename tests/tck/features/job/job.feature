Feature: Job

  @skip
  Scenario: submit snapshot job
    Given a graph with data "nba" named "nba_snapshot"
    When submitting job:
      """
      SUBMIT JOB SNAPSHOT
      """
    Then the result should be, in any order, with "10" seconds timout:
      | Job Id | Command  | Status   |
      | r"\d+" | SNAPSHOT | FINISHED |
    And no side effects
    When executing query:
      """
      MATCH (v:player {age: 29})
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |
    And no side effects

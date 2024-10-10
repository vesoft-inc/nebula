Feature: Test excute index scan with label which contains parameters

  Background:
    Given an empty graph
    And load "nba" csv data to a new space
    Given parameters: {"p1":"Tim Duncan"}

  Scenario: execute index scan with label which contains parameters
    When profiling query:
      """
      MATCH (v:player{name:$p1})  RETURN v.player.age
      """
    Then the result should be, in any order:
      | v.player.age |
      | 42           |
    Then the execution plan should be:
      | id | name           | dependencies | operator info                                       |
      | 4  | Project        | 3            |                                                     |
      | 3  | AppendVertices | 2            |                                                     |
      | 2  | IndexScan      | 1            | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 1  | Start          |              |                                                     |

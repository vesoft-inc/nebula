Feature: Fix #4938

  Background:
    Given a graph with space named "nba"

  Scenario: fix #4938
    When profiling query:
      """
      MATCH (a:player)
      WHERE id(a)=='Tim Duncan'
      MATCH (a)-[:like]-(b)
      RETURN count(*) AS cnt
      """
    Then the result should be, in any order:
      | cnt |
      | 12  |
    And the execution plan should be:
      | id | name           | dependencies | operator info |
      | 11 | Aggregate      | 10           |               |
      | 10 | BiInnerJoin    | 9,4          |               |
      | 9  | Project        | 8            |               |
      | 8  | AppendVertices | 7            |               |
      | 7  | Dedup          | 6            |               |
      | 6  | PassThrough    | 5            |               |
      | 5  | Start          |              |               |
      | 4  | Project        | 3            |               |
      | 3  | AppendVertices | 2            |               |
      | 2  | Traverse       | 1            |               |
      | 1  | Argument       | 0            |               |
      | 0  | Start          |              |               |
